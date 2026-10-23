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

package cloud.elastic.dlagent.api.utilities;

import java.util.function.Function;

/**
 * Container-neutral static service locator, the replacement for the former
 * {@code SpringContext}.
 * <p>
 * This is deliberately <em>not</em> dependency injection and cannot be replaced
 * by {@code javax.inject.Provider}: the three {@code *MetadataFetcher} classes
 * are created reflectively by
 * {@code BasePluginFactory.getPlugin(..)} through
 * {@code Class.forName(name).getConstructor().newInstance()} with the class
 * name coming from the profile configuration, so no container participates in
 * their construction and there is nowhere to inject a provider into.
 * Their no-arg constructors therefore have to reach back
 * into the container, and the only portable way to do that is a static locator.
 * <p>
 * Each composition root installs its own resolver during startup phase 1 --
 * {@code ApplicationContext::getBean} for dlagent-rest,
 * {@code Injector::getInstance} for dlagent-grpc -- which must happen before
 * any plugin is reflectively created, i.e. before the agent reports itself
 * ready.
 */
public final class BeanLocator {

    private static volatile Function<Class<?>, Object> resolver;

    private BeanLocator() {
    }

    /**
     * Installs the container's lookup function. Called once per process by the
     * composition root before any request is served.
     *
     * @param newResolver maps a required type to the container-managed instance
     */
    public static void setResolver(Function<Class<?>, Object> newResolver) {
        if (newResolver == null) {
            throw new IllegalArgumentException("bean resolver must not be null");
        }
        resolver = newResolver;
    }

    /**
     * Drops the installed resolver. Only used by tests, so that one test's
     * container cannot leak into the next.
     */
    public static void clearResolver() {
        resolver = null;
    }

    /**
     * Returns the container-managed instance of the given type.
     *
     * @param requiredType type the instance must match; can be an interface or superclass
     * @param <T>          the required type
     * @return the instance the container holds for that type
     */
    public static <T> T getBean(Class<T> requiredType) {
        Function<Class<?>, Object> current = resolver;
        if (current == null) {
            throw new IllegalStateException(
                    "no bean resolver installed; the composition root must call "
                            + "BeanLocator.setResolver(..) before any plugin is created");
        }
        return requiredType.cast(current.apply(requiredType));
    }
}
