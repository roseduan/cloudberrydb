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

package cloud.elastic.dlagent.service.utilities;

import org.apache.commons.lang.StringUtils;
import cloud.elastic.dlagent.api.model.Plugin;
import cloud.elastic.dlagent.api.model.RequestContext;
import javax.inject.Named;
import javax.inject.Singleton;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * The base implementation of the {@code PluginFactory}
 */
@Named
@Singleton
public class BasePluginFactory {

    public <T extends Plugin> T getPlugin(RequestContext context, String pluginClassName) {

        // get the class name of the plugin
        if (StringUtils.isBlank(pluginClassName)) {
            throw new RuntimeException("Could not determine plugin class name");
        }

        // load the class by name
        Class<?> cls;
        try {
            cls = Class.forName(pluginClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Class %s is not found", pluginClassName), e);
        }

        // check if the class is a plugin
        if (!Plugin.class.isAssignableFrom(cls)) {
            throw new RuntimeException(String.format("Class %s does not implement Plugin interface", pluginClassName));
        }

        // get the empty constructor
        Constructor<?> con;
        try {
            con = cls.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format("Class %s does not have an empty constructor", pluginClassName));
        }

        // create plugin instance
        Plugin instance;
        try {
            instance = (Plugin) con.newInstance();
        } catch (InvocationTargetException e) {
            throw (e.getCause() != null) ? new RuntimeException(e.getCause()) :
                    new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Class %s could not be instantiated", pluginClassName), e);
        }

        // initialize the instance
        instance.setRequestContext(context);
        instance.afterPropertiesSet();

        // cast into a target type
        @SuppressWarnings("unchecked")
        T castInstance = (T) instance;

        return castInstance;
    }
}
