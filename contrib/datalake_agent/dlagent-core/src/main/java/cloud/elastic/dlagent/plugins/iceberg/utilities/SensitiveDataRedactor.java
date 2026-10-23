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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Recursively masks values whose key identifies credential material. */
public final class SensitiveDataRedactor {
    private SensitiveDataRedactor() {
    }

    public static Object redact(Object value) {
        if (value instanceof Map) {
            Map<?, ?> source = (Map<?, ?>) value;
            Map<Object, Object> safe = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : source.entrySet()) {
                Object key = entry.getKey();
                safe.put(key, isSensitiveKey(String.valueOf(key))
                        ? "**" : redact(entry.getValue()));
            }
            return safe;
        }
        if (value instanceof Iterable) {
            List<Object> safe = new ArrayList<>();
            for (Object item : (Iterable<?>) value) {
                safe.add(redact(item));
            }
            return safe;
        }
        if (value != null && value.getClass().isArray()) {
            List<Object> safe = new ArrayList<>();
            for (int i = 0; i < Array.getLength(value); i++) {
                safe.add(redact(Array.get(value, i)));
            }
            return safe;
        }
        return value;
    }

    /**
     * Separators vary by key family -- {@code access_key_id},
     * {@code fs.s3a.access.key} and {@code s3.access-key-id} all name the same
     * thing -- so they are collapsed before matching rather than enumerated.
     */
    private static boolean isSensitiveKey(String key) {
        String normalized = key.toLowerCase(Locale.ROOT).replaceAll("[-_. ]", "");
        /*
         * "access" on its own is not credential material: it also spells the
         * addressing flag (s3.path-style-access) and the delegation header
         * (header.X-Iceberg-Access-Delegation).  Masking those blanks out the
         * two fields you most need when a gateway answers 403, so the secret
         * is required to be an access KEY.
         */
        return normalized.contains("accesskey")
                || normalized.contains("secret")
                || normalized.contains("token")
                || normalized.contains("credential")
                || normalized.contains("password")
                || normalized.contains("keytab");
    }
}
