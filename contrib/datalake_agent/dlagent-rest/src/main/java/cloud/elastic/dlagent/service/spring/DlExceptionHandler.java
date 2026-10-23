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

import cloud.elastic.dlagent.api.error.DlRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Handler for dlagent specific exceptions that just reports the request error status. The actual message body with
 * all proper error attributes is created by the Spring MVC BasicErrorController.
 * <p>
 * This handler prevents the dlagent specific exception from being thrown to the container, where it would've gotten
 * logged without an MDC context, since by that time the MDC context is cleaned up.
 * <p>
 * Instead, it is assumed that the dlagent specific exception has been seen by the the dlagent resource
 * or the processing logic and was logged there, where the MDC context is still available.
 */
@ControllerAdvice
public class DlExceptionHandler {

    @ExceptionHandler({DlRuntimeException.class})
    public void handleDlAgentRuntimeException(DlRuntimeException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
    }

}
