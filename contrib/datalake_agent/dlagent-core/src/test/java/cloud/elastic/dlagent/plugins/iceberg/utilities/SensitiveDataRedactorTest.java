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

package cloud.elastic.dlagent.plugins.iceberg.utilities;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The redactor decides what a support engineer can see in a log.  Both
 * directions are failures: a leaked key is a security incident, and a masked
 * addressing flag turns the catalog-properties log line -- the one artifact
 * that says how we talked to the gateway -- into a row of asterisks.
 */
class SensitiveDataRedactorTest {

    @SuppressWarnings("unchecked")
    private Map<String, Object> redact(Map<String, String> in) {
        return (Map<String, Object>) SensitiveDataRedactor.redact(in);
    }

    private Map<String, String> map(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void masksCredentialMaterial() {
        Map<String, Object> out = redact(map(
                "s3.access-key-id", "AKIAREAL",
                "s3.secret-access-key", "shhh",
                "s3.session-token", "tok",
                "rest.access-key-id", "AKIAREAL",
                "client_secret", "shhh",
                "gopher.krb_delegation_token", "tok",
                "hive.keytab", "/etc/k.keytab",
                "password", "hunter2",
                "storage-credentials", "blob"));
        for (Map.Entry<String, Object> e : out.entrySet()) {
            assertEquals("**", e.getValue(), "must be masked: " + e.getKey());
        }
    }

    /**
     * Addressing and delegation flags carry no secret.  They are exactly what
     * you need to read when a gateway answers 403: did we send path-style, and
     * did we actually ask for vended credentials?
     */
    @Test
    void keepsNonSecretFlagsThatMerelyContainAccess() {
        Map<String, Object> out = redact(map(
                "s3.path-style-access", "true",
                "path_style_access", "true",
                "fs.s3a.path.style.access", "true",
                "fs.s3.path.style.access", "true",
                "header.X-Iceberg-Access-Delegation", "vended-credentials"));

        assertEquals("true", out.get("s3.path-style-access"));
        assertEquals("true", out.get("path_style_access"));
        assertEquals("true", out.get("fs.s3a.path.style.access"));
        assertEquals("true", out.get("fs.s3.path.style.access"));
        assertEquals("vended-credentials", out.get("header.X-Iceberg-Access-Delegation"));
    }
}
