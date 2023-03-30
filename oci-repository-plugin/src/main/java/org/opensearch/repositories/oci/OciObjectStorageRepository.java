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

import static org.opensearch.common.settings.Setting.Property;
import static org.opensearch.common.settings.Setting.boolSetting;
import static org.opensearch.common.settings.Setting.byteSizeSetting;
import static org.opensearch.common.settings.Setting.simpleString;

import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

/** Blob repository that corresponds to OCI */
@Log4j2
public class OciObjectStorageRepository extends BlobStoreRepository {

    // package private for testing
    static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(100, ByteSizeUnit.MB);

    public static final String TYPE = "oci";

    public static final Setting<String> BUCKET_SETTING =
            simpleString("bucket", Property.NodeScope, Property.Dynamic);
    public static final Setting<String> NAMESPACE_SETTING =
            simpleString("namespace", Property.NodeScope, Property.Dynamic);
    // Force repository bucket creation if not exists
    public static final Setting<Boolean> FORCE_BUCKET_CREATION_SETTING =
            boolSetting(
                    "forceBucketCreation",
                    false,
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic);
    // if force creating buckets need to provide the compartment ID under which they will be created
    public static final Setting<String> BUCKET_COMPARTMENT_ID_SETTING =
            simpleString("bucket_compartment_id", Property.NodeScope, Property.Dynamic);
    public static final Setting<String> BASE_PATH_SETTING =
            simpleString("base_path", Property.NodeScope, Property.Dynamic);
    public static final Setting<Boolean> COMPRESS =
            boolSetting("compress", false, Property.NodeScope, Property.Dynamic);
    public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING =
            byteSizeSetting(
                    "chunk_size",
                    MAX_CHUNK_SIZE,
                    MIN_CHUNK_SIZE,
                    MAX_CHUNK_SIZE,
                    Property.NodeScope,
                    Property.Dynamic);
    static final Setting<String> CLIENT_NAME_SETTINGS =
            new Setting<>("client", "default", Function.identity());

    private final OciObjectStorageService storageService;
    private final BlobPath basePath;
    private final ByteSizeValue chunkSize;

    OciObjectStorageRepository(
            final RepositoryMetadata metadata,
            final NamedXContentRegistry namedXContentRegistry,
            final OciObjectStorageService storageService,
            final ClusterService clusterService,
            final RecoverySettings recoverySettings) {
        super(
                metadata,
                getSetting(COMPRESS, metadata),
                namedXContentRegistry,
                clusterService,
                recoverySettings);

        this.storageService = storageService;
        String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }

        this.chunkSize = getSetting(CHUNK_SIZE_SETTING, metadata);
    }

    @Override
    protected OciObjectStorageBlobStore createBlobStore() {
        return new OciObjectStorageBlobStore(storageService, metadata);
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    /**
     * Get a given setting from the repository settings, throwing a {@link RepositoryException} if
     * the setting does not exist or is empty.
     */
    static <T> T getSetting(Setting<T> setting, RepositoryMetadata metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(
                    metadata.name(),
                    "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if (value instanceof String && !Strings.hasText((String) value)) {
            throw new RepositoryException(
                    metadata.name(), "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }
}
