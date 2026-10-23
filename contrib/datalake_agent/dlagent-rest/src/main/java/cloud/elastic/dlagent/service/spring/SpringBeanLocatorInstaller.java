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

import cloud.elastic.dlagent.api.utilities.BeanLocator;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Installs the Spring lookup into {@link BeanLocator}, the replacement for the
 * former {@code SpringContext} helper.
 * <p>
 * Same shape as before -- eager {@code @Component} implementing
 * {@code ApplicationContextAware} -- so the resolver is in place well before
 * the first request reflectively creates a {@code *MetadataFetcher}. What
 * changed is only that the static holder now lives in dlagent-core and takes
 * a plain lookup function, so dlagent-grpc can install Guice's
 * {@code Injector::getInstance} there instead.
 */
@Lazy(value = false)
@Component
public class SpringBeanLocatorInstaller implements ApplicationContextAware {

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        BeanLocator.setResolver(context::getBean);
    }
}
