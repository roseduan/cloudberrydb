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

package cloud.elastic.dlagent.service.security;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.security.PrivilegedAction;

/**
 * Security Service
 */
@Named
@Singleton
public class BaseSecurityService implements SecurityService {

    private static final Logger LOG = LoggerFactory.getLogger(BaseSecurityService.class);

    private final SecureLogin secureLogin;
    private final UGIProvider ugiProvider;

    @Inject
    public BaseSecurityService(SecureLogin secureLogin, UGIProvider ugiProvider) {
        this.secureLogin = secureLogin;
        this.ugiProvider = ugiProvider;
    }

    /**
     * If user impersonation is configured, examines the request for the
     * presence of the expected security headers and create a proxy user to
     * execute further request chain. If security is enabled for the
     * configuration server used for the requests, makes sure that a login
     * UGI for the the Kerberos principal is created.
     *
     * <p>Responds with an HTTP error if the header is missing or the chain
     * processing throws an exception.
     *
     * @param context the context for the given request
     * @param action  the action to be executed
     * @throws Exception if the operation fails
     */
    public <T> T doAs(RequestContext context, PrivilegedAction<T> action) throws Exception {
        // retrieve user header and make sure header is present and is not empty
        final String gpdbUser = context.getUser();
        final String serverName = context.getServerName();
        final String configDirectory = context.getConfig();
        final Configuration configuration = context.getConfiguration();
        final boolean isUserImpersonationEnabled = secureLogin.isUserImpersonationEnabled(configuration);
        final boolean isSecurityEnabled = Utilities.isSecurityEnabled(configuration);

        // Establish the UGI for the login user or the Kerberos principal for the given server, if applicable
        boolean exceptionDetected = false;
        UserGroupInformation userGroupInformation = null;
        try {
            /*
               get a login user that is either of:
               - (non-secured cluster) - a user running the dlagent process
               - (non-secured cluster) - a user specified by dlagent.service.user.name
               - (secured cluster)     - a Kerberos principal
             */
            UserGroupInformation loginUser = secureLogin.getLoginUser(serverName, configDirectory, configuration);

            // start with assuming identity of the dlagent service to be the same as the login user
            String serviceUser = loginUser.getUserName();

            // establish the identity of the remote user that will be presented to the backend system
            String remoteUser = (isUserImpersonationEnabled ? gpdbUser : serviceUser);

            // Retrieve proxy user UGI from the UGI of the logged in user
            if (isUserImpersonationEnabled) {
                LOG.debug("Creating proxy user = {} for real user {}", remoteUser, loginUser);
                userGroupInformation = ugiProvider.createProxyUser(remoteUser, loginUser);
            } else {
                LOG.debug("Creating remote user = {} for real user {}", remoteUser, loginUser);
                userGroupInformation = ugiProvider.createRemoteUser(remoteUser, loginUser, isSecurityEnabled);
            }

            LOG.debug("Retrieved proxy user {} for server {}", userGroupInformation, serverName);
            LOG.info("Performing request for gpdb_user = {} as [remote_user={}, service_user={}, login_user={}] with{} impersonation",
                    gpdbUser, remoteUser, serviceUser, loginUser.getUserName(), isUserImpersonationEnabled ? "" : "out");
            // Execute the servlet chain as that user
            return userGroupInformation.doAs(action);
        } catch (Exception e) {
            exceptionDetected = true;
            throw e;
        } finally {
            LOG.debug("Releasing UGI resources. {}", exceptionDetected ? " Exception while processing." : "");
            try {
                if (userGroupInformation != null) {
                    ugiProvider.destroy(userGroupInformation);
                }
            } catch (Throwable t) {
                LOG.warn("Error releasing UGI resources, ignored.", t);
            }
        }
    }
}
