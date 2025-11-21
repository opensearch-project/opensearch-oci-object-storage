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

import static org.opensearch.common.settings.Setting.boolSetting;
import static org.opensearch.common.settings.Setting.simpleString;
import static org.opensearch.repositories.oci.OciObjectStorageRepository.*;

import java.io.InputStream;
import java.nio.file.Files;
import java.util.function.Supplier;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.Region;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;

/** Container for OCI object storage clients settings. */
@Log4j2
public class OciObjectStorageClientSettings {
    public static final String DEV_REGION = "us-ashburn-1";

    /** Path to a credentials file */
    public static final Setting<String> CREDENTIALS_FILE_SETTING =
            simpleString("credentials_file", Setting.Property.NodeScope, Setting.Property.Dynamic);

    /** An override for the Object Storage endpoint to connect to. */
    public static final Setting<String> ENDPOINT_SETTING =
            simpleString("endpoint", Setting.Property.NodeScope, Setting.Property.Dynamic);

    /** An override for the region. */
    public static final Setting<String> REGION_SETTING =
            simpleString("region", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> USER_ID_SETTING =
            simpleString("userId", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> TENANT_ID_SETTING =
            simpleString("tenantId", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> FINGERPRINT_SETTING =
            simpleString("fingerprint", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Boolean> INSTANCE_PRINCIPAL =
            boolSetting(
                    "useInstancePrincipal",
                    false,
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic);

    /** The credentials used by the client to connect to the Storage endpoint. */
    private final BasicAuthenticationDetailsProvider authenticationDetailsProvider;

    private final String clientName;

    /** The Storage endpoint URL the client should talk to. Null value sets the default. */
    private final String endpoint;

    private final boolean isInstancePrincipal;

    private String userId;
    private String tenantId;
    private String fingerprint;
    private String credentialsFilePath;

    /** The region to access storage service */
    private Region region;

    OciObjectStorageClientSettings(final RepositoryMetadata metadata) {
        Settings settings = metadata.settings();
        this.clientName = CLIENT_NAME_SETTINGS.get(settings);
        this.endpoint = OciObjectStorageRepository.getSetting(ENDPOINT_SETTING, metadata);
        isInstancePrincipal = OciObjectStorageRepository.getSetting(INSTANCE_PRINCIPAL, metadata);

        // If we are not using instance principal we are going to have to provide user principal
        // info
        if (!isInstancePrincipal) {
            this.userId = OciObjectStorageRepository.getSetting(USER_ID_SETTING, metadata);
            this.tenantId = OciObjectStorageRepository.getSetting(TENANT_ID_SETTING, metadata);
            this.fingerprint = OciObjectStorageRepository.getSetting(FINGERPRINT_SETTING, metadata);
            this.credentialsFilePath =
                    OciObjectStorageRepository.getSetting(CREDENTIALS_FILE_SETTING, metadata);
            String regionStr = OciObjectStorageRepository.getSetting(REGION_SETTING, metadata);

            final Region region = Region.fromRegionCodeOrId(regionStr);

            log.info(
                    "Initializing client settings with:\n"
                            + " userId: {}\n"
                            + " tenantId: {}\n"
                            + " fingerPrint: {}\n"
                            + " credentialsFilePath {}\n"
                            + "region: {}\n",
                    userId,
                    tenantId,
                    fingerprint,
                    credentialsFilePath,
                    region);
            this.authenticationDetailsProvider =
                    toAuthDetailsProvider(
                            () -> {
                                try {
                                    return Files.newInputStream(PathUtils.get(credentialsFilePath));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            region,
                            userId,
                            tenantId,
                            fingerprint);
        } else {
            // We are using instance principal and therefore we are going to use instance principal
            // provider
            log.info("Initializing client using instance principals");
            this.authenticationDetailsProvider = toAuthDetailsProvider();
        }
    }

    public String getClientName() {
        return clientName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public BasicAuthenticationDetailsProvider getAuthenticationDetailsProvider() {
        return authenticationDetailsProvider;
    }

    private static BasicAuthenticationDetailsProvider toAuthDetailsProvider(
            Supplier<InputStream> privateKeySupplier,
            Region region,
            String userId,
            String tenantId,
            String fingerprint) {
        /*
         * The SDK's "region code" is the internal enum's public region name.
         */

        return SocketAccess.doPrivilegedIOException(
                () ->
                        SimpleAuthenticationDetailsProvider.builder()
                                .userId(userId)
                                .tenantId(tenantId)
                                .region(region)
                                .fingerprint(fingerprint)
                                .privateKeySupplier(privateKeySupplier)
                                .build());
    }

    private static BasicAuthenticationDetailsProvider toAuthDetailsProvider() {
        try {
            return SocketAccess.doPrivilegedIOException(
                    () -> InstancePrincipalsAuthenticationDetailsProvider.builder().build());
        } catch (Exception ex) {
            log.error("Failure calling toAuthDetailsProvider", ex);
            throw ex;
        }
    }

    public boolean isInstancePrincipal() {
        return isInstancePrincipal;
    }

    public Region getRegion() {
        return region;
    }

    public String getUserId() {
        return userId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public String getCredentialsFilePath() {
        return credentialsFilePath;
    }
}
