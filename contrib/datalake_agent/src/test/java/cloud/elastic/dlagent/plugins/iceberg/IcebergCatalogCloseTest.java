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

package cloud.elastic.dlagent.plugins.iceberg;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * {@link IcebergCatalog#close()} has a no-op default, which is only correct for
 * an implementation that holds nothing open.  Every implementation here holds a
 * FileIO -- under gopher a GopherFileIO with a native connection -- and is built
 * fresh per request, so inheriting the default leaks one connection per write
 * until the agent runs out of descriptors.
 *
 * <p>Measured before this was fixed: ten writes through the built-in catalog
 * initialised ten GopherFileIOs and closed none, and the agent JVM gained
 * roughly one socket per write with no plateau, while the rest catalog -- which
 * did override close() -- stayed flat.
 *
 * <p>This is the cheapest possible guard, and it is aimed at the way the bug
 * actually arrived: three implementations simply never said anything about
 * close(), so nothing anywhere pointed at them.
 */
class IcebergCatalogCloseTest {

    private static final List<Class<? extends IcebergCatalog>> IMPLEMENTATIONS =
            Arrays.asList(
                    IcebergBuildInCatalog.class,
                    IcebergS3Catalog.class,
                    IcebergHadoopCatalog.class,
                    IcebergRestCatalog.class,
                    IcebergPolarisCatalog.class,
                    IcebergHiveCatalog.class,
                    InvalidatingHiveCatalog.class);

    /**
     * Keeps the assertion below from passing vacuously: an implementation that
     * does not override close() reports the interface as the declaring class,
     * which is exactly what the next test rejects.
     */
    @Test
    void inheritedCloseIsAttributedToTheInterface() throws Exception {
        assertEquals(IcebergCatalog.class,
                IcebergCatalog.class.getMethod("close").getDeclaringClass());
    }

    @Test
    void everyCatalogReleasesWhatItHolds() throws Exception {
        for (Class<? extends IcebergCatalog> impl : IMPLEMENTATIONS) {
            Class<?> declaring = impl.getMethod("close").getDeclaringClass();
            assertNotEquals(IcebergCatalog.class, declaring,
                    impl.getSimpleName() + " inherits the no-op close(); its FileIO "
                            + "is never released and leaks one connection per request");
        }
    }
}
