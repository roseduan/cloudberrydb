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

package cloud.elastic.dlagent.api.error;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

public class DlRuntimeException extends RuntimeException {

    @Getter
    private final String hint;

    public DlRuntimeException() {
        this(null, null, null);
    }

    public DlRuntimeException(String message) {
        this(message, null, null);
    }

    public DlRuntimeException(String message, String hint) {
        this(message, hint, null);
    }

    public DlRuntimeException(Throwable cause) {
        this(StringUtils.defaultIfBlank(cause.getMessage(), cause.getClass().getName()), cause);
    }

    public DlRuntimeException(String message, Throwable cause) {
        this(message, null, cause);
    }

    public DlRuntimeException(String message, String hint, Throwable cause) {
        super(message, cause);
        this.hint = hint;
    }

}
