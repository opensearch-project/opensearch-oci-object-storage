/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.oci;

import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.http.client.HttpProvider;
import com.oracle.bmc.http.client.jersey.JerseyHttpProvider;
import com.oracle.bmc.objectstorage.ObjectStorageAsync;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.crypto.CryptoUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.AllPermission;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.repositories.oci.OciObjectStorageClientSettings.DEV_REGION;

/**
 * The plugin class
 */
public class OciObjectStoragePlugin extends Plugin implements RepositoryPlugin {

    // package-private for tests
    final OciObjectStorageService storageService;
    final Settings settings;

    public OciObjectStoragePlugin(final Settings settings) {
        this.storageService = createStorageService();
        this.settings = settings;

        /*
         * Work around security bug while using Instance Principal Authentication
         * where adding the required java.net.SocketPermission to oci-repository-plugin/src/main/resources/plugin-security.policy
         * doesn't fix the issue. See https://github.com/opensearch-project/opensearch-oci-object-storage/issues/13
         */
        AccessController.doPrivileged(
                (PrivilegedAction<Object>)
                        () -> {
                            System.setSecurityManager(
                                    new SecurityManager() {

                                        @Override
                                        public void checkPermission(Permission perm) {
                                            if (perm instanceof AllPermission) {
                                                throw new SecurityException();
                                            }
                                        }
                                    });
                            return null;
                        });


        // Hack to force Jersey to load first as a default provider
        HttpProvider.getDefault();
    }

    // overridable for tests
    protected OciObjectStorageService createStorageService() {
        return new OciObjectStorageService();
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings) {
        return Collections.singletonMap(
                OciObjectStorageRepository.TYPE,
                metadata ->
                        new OciObjectStorageRepository(
                                metadata,
                                namedXContentRegistry,
                                this.storageService,
                                clusterService,
                                recoverySettings));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
                OciObjectStorageClientSettings.CREDENTIALS_FILE_SETTING,
                OciObjectStorageClientSettings.ENDPOINT_SETTING,
                OciObjectStorageClientSettings.REGION_SETTING,
                OciObjectStorageClientSettings.FINGERPRINT_SETTING,
                OciObjectStorageClientSettings.INSTANCE_PRINCIPAL,
                OciObjectStorageClientSettings.TENANT_ID_SETTING,
                OciObjectStorageClientSettings.USER_ID_SETTING,
                OciObjectStorageRepository.BASE_PATH_SETTING,
                OciObjectStorageRepository.BUCKET_SETTING,
                OciObjectStorageRepository.BUCKET_COMPARTMENT_ID_SETTING,
                OciObjectStorageRepository.FORCE_BUCKET_CREATION_SETTING,
                OciObjectStorageRepository.NAMESPACE_SETTING);
    }
}
