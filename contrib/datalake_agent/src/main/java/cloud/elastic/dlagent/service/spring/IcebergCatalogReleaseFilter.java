/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent.service.spring;

import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Closes the Iceberg catalogs a request built, once that request is over.
 *
 * <p>Every catalog type other than hive is constructed fresh per request, and
 * the rest / polaris ones carry a GopherFileIO whose connection to the
 * gophermeta worker stays open until something closes it. Nothing did, so the
 * agent accumulated one connection per request until gophermeta ran out of
 * file descriptors -- at which point queries stopped failing and started
 * hanging instead, waiting on replies that never came.
 *
 * <p>The request boundary is where this belongs rather than the call sites:
 * the {@code Table} a caller receives reads through the catalog's FileIO, so
 * the catalog has to outlive the entire handler, and nothing outside the
 * request may touch it. Doing it here also means a new call site cannot
 * reintroduce the leak by forgetting.
 */
@Slf4j
@Component
public class IcebergCatalogReleaseFilter extends OncePerRequestFilter {

    private final IcebergCatalogWrapper catalogWrapper;

    public IcebergCatalogReleaseFilter(IcebergCatalogWrapper catalogWrapper) {
        this.catalogWrapper = catalogWrapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain)
            throws ServletException, IOException {
        boolean served = false;

        try {
            chain.doFilter(request, response);
            served = true;
        } finally {
            /*
             * Send the answer before cleaning up.  Closing a GopherFileIO waits
             * on the gophermeta worker, which a wedged earlier request can hold
             * for as long as the agent request timeout allows; a caller whose
             * work is finished should not be kept waiting on that.  Flushing
             * commits the response, so it is done only when the handler
             * returned normally -- while an exception is unwinding, the error
             * response has not been written yet and committing here would lose
             * it.
             *
             * This shortens the wait, it does not remove the stall: the native
             * layer admits one operation at a time, so a close that blocks
             * still delays whichever request needs that slot next.  That is a
             * property of the storage client, not something this filter can
             * schedule its way out of.
             */
            if (served) {
                try {
                    response.flushBuffer();
                } catch (IOException | RuntimeException e) {
                    log.debug("Could not flush the response before catalog cleanup", e);
                }
            }

            /*
             * Never let cleanup turn a served request into a failed one, nor
             * mask the exception the handler is already unwinding with.
             */
            try {
                catalogWrapper.releaseRequestCatalogs();
            } catch (RuntimeException e) {
                log.warn("Failed to release per-request iceberg catalogs", e);
            }
        }
    }
}
