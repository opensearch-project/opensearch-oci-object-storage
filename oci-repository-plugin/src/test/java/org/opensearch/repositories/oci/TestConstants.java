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

import org.opensearch.common.crypto.CryptoUtils;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import static org.opensearch.repositories.oci.OciObjectStorageClientSettings.DEV_REGION;

public class TestConstants {
    public static Settings getRepositorySettings() throws NoSuchAlgorithmException, IOException {
        return getRepositorySettings("http://localhost:8080");
    }

    public static Settings getRepositorySettings(String url) throws NoSuchAlgorithmException, IOException {
        final Path keyFile = CryptoUtils.generatePrivatePublicKeyPair();

        return Settings.builder()
                .put(
                        OciObjectStorageClientSettings.ENDPOINT_SETTING.getKey(),
                        url)
                .put(OciObjectStorageClientSettings.REGION_SETTING.getKey(), DEV_REGION)
                .put(OciObjectStorageClientSettings.CREDENTIALS_FILE_SETTING.getKey(), keyFile)
                .put(OciObjectStorageClientSettings.INSTANCE_PRINCIPAL.getKey(), false)
                .put(OciObjectStorageClientSettings.FINGERPRINT_SETTING.getKey(), "fingerprint")
                .put(OciObjectStorageClientSettings.TENANT_ID_SETTING.getKey(), "tenantId")
                .put(OciObjectStorageClientSettings.USER_ID_SETTING.getKey(), "userId")
                .put(OciObjectStorageRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
                .put(
                        OciObjectStorageRepository.BUCKET_COMPARTMENT_ID_SETTING.getKey(),
                        "bucket_compartment_id")
                .put(OciObjectStorageRepository.BUCKET_SETTING.getKey(), "mySnapshotsBucket")
                .put(OciObjectStorageRepository.FORCE_BUCKET_CREATION_SETTING.getKey(), true)
                .put(
                        OciObjectStorageRepository.NAMESPACE_SETTING.getKey(),
                        "myObjectStorageNamespace")
                .build();
    }
}
