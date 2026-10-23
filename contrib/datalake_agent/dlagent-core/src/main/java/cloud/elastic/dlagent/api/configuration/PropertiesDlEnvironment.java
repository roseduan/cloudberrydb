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

package cloud.elastic.dlagent.api.configuration;

import java.time.Duration;
import java.util.Properties;

/**
 * {@link DlEnvironment} over a plain {@link Properties} instance, used by the
 * containers that have no Spring {@code Environment} (dlagent-grpc, and unit
 * tests of core classes).
 * <p>
 * Only the conversions core actually asks for are implemented; an unsupported
 * target type fails loudly rather than silently returning the default, so that
 * a property cannot be lost by accident. The full
 * relaxed-binding / interpolation / precedence machinery lives in
 * dlagent-grpc's {@code DlConfig} -- this class only decodes an
 * already-resolved property value.
 */
public class PropertiesDlEnvironment implements DlEnvironment {

    private final Properties properties;

    /**
     * Creates an environment reading from the given properties.
     *
     * @param properties resolved properties, never null
     */
    public PropertiesDlEnvironment(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        this.properties = properties;
    }

    @Override
    public <T> T getProperty(String name, Class<T> targetType, T defaultValue) {
        String raw = properties.getProperty(name);
        if (raw == null) {
            return defaultValue;
        }
        return targetType.cast(convert(name, raw.trim(), targetType));
    }

    private Object convert(String name, String raw, Class<?> targetType) {
        if (targetType == String.class) {
            return raw;
        }
        if (targetType == Boolean.class || targetType == boolean.class) {
            return Boolean.valueOf(raw);
        }
        try {
            if (targetType == Integer.class || targetType == int.class) {
                return Integer.valueOf(raw);
            }
            if (targetType == Long.class || targetType == long.class) {
                return Long.valueOf(raw);
            }
            if (targetType == Double.class || targetType == double.class) {
                return Double.valueOf(raw);
            }
            if (targetType == Duration.class) {
                return Duration.parse(raw);
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(String.format(
                    "property %s: cannot convert '%s' to %s", name, raw, targetType.getName()), e);
        }
        throw new IllegalArgumentException(String.format(
                "property %s: unsupported target type %s", name, targetType.getName()));
    }
}
