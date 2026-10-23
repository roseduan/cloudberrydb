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

package cloud.elastic.dlagent.api.model;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, ...).
 * Manages the meta data.
 */
public class BasePlugin implements Plugin {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Configuration configuration;
    protected RequestContext context;

    /**
     * {@inheritDoc}
     */
    public void setRequestContext(RequestContext context) {
        this.context = context;
        this.configuration = context.getConfiguration();
    }

    /**
     * Method called after the {@link RequestContext} and {@link Configuration}
     * have been bound to the BasePlugin and is ready to be consumed by
     * implementing classes
     */
    @Override
    public void afterPropertiesSet() {
    }
}
