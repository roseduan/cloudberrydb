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

import org.apache.coyote.ProtocolHandler;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import cloud.elastic.dlagent.api.configuration.DlServerProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

/**
 * The {@link DlTomcatCustomizer} class allows customizing application container
 * properties that are not exposed through the application.properties file.
 * For example, setting the max header count or the http header size.
 */
@Component
public class DlTomcatCustomizer implements
        WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

    private final DlServerProperties serverProperties;

    /**
     * Create a new DlAgentCustomContainer with the given server properties
     *
     * @param serverProperties the server properties
     */
    public DlTomcatCustomizer(DlServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    @Override
    public void customize(TomcatServletWebServerFactory factory) {
        factory.addConnectorCustomizers(connector -> {
            ProtocolHandler handler = connector.getProtocolHandler();
            if (handler instanceof AbstractHttp11Protocol) {
                AbstractHttp11Protocol<?> protocolHandler = (AbstractHttp11Protocol<?>) handler;
                protocolHandler.setMaxHeaderCount(serverProperties.getTomcat().getMaxHeaderCount());
                protocolHandler.setDisableUploadTimeout(serverProperties.getTomcat().isDisableUploadTimeout());
                protocolHandler.setConnectionUploadTimeout((int) serverProperties.getTomcat().getConnectionUploadTimeout().toMillis());
            }
        });
    }
}
