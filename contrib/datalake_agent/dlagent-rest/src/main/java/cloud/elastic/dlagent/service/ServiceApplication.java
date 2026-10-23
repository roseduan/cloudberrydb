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

package cloud.elastic.dlagent.service;

import org.apache.hadoop.security.DlUserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Main Agent Spring Configuration class.
 */
@SpringBootApplication(scanBasePackages = "cloud.elastic.dlagent", scanBasePackageClasses = DlUserGroupInformation.class)
@EnableScheduling
public class ServiceApplication {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceApplication.class);

    /**
     * Constructs a new ServiceApplication
     */
    public ServiceApplication() {
        logClassLoaderInfo();
    }

    /**
     * Logs, at info level, all the libraries loaded by the ClassLoader used by
     * the ServiceApplication.
     */
    private void logClassLoaderInfo() {
        ClassLoader loader = this.getClass().getClassLoader();
        if (loader instanceof URLClassLoader) {
            URLClassLoader urlClassLoader = (URLClassLoader) loader;
            URL[] urls = urlClassLoader.getURLs();
            if (urls != null) {
                for (URL url : urls) {
                    LOG.info("Added repository {}", url);
                }
            }
        }
    }

    /**
     * Spring Boot Main.
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(ServiceApplication.class, args);
    }

}
