/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import javax.jcr.RepositoryException;
import java.security.Principal;

public class MountPermissionRootProvider implements PermissionRootProvider {

    private final NodeBuilder permissionStore;

    private final String workspaceName;

    private final MountInfoProvider mountInfoProvider;

    private final PrincipalProvider principalProvider;

    private final UserManager userManager;

    public MountPermissionRootProvider(NodeBuilder permissionStore, String workspaceName, MountInfoProvider mountInfoProvider, PrincipalProvider principalProvider, UserManager userManager) {
        this.permissionStore = permissionStore;
        this.workspaceName = workspaceName;
        this.mountInfoProvider = mountInfoProvider;
        this.principalProvider = principalProvider;
        this.userManager = userManager;
    }

    @Override
    public NodeBuilder getPermissionRoot(String principalName) throws RepositoryException {
        Principal principal = principalProvider.getPrincipal(principalName);
        Authorizable authorizable = userManager.getAuthorizable(principal);
        String path = authorizable.getPath();
        Mount principalMount = mountInfoProvider.getMountByPath(path);
        return permissionStore.getChildNode(MountPermissionProvider.getPermissionRootName(principalMount, workspaceName));
    }
}
