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

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.opensearch.repositories.oci.OciObjectStorageRepository.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.log4j.Log4j2;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.model.BmcException;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.model.Range;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.ObjectStorageClient;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.model.ObjectSummary;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.requests.*;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.responses.ListObjectsResponse;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.transfer.DownloadConfiguration;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.transfer.DownloadManager;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.transfer.UploadManager;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.retrier.RetryConfiguration;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.waiter.ExponentialBackoffDelayStrategy;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.waiter.MaxAttemptsTerminationStrategy;

// Lots of code duplication with OciObjectStorageBlobStore and OciObjectStorageBlobContainer
// Re-using the code will require significant refactoring which is needed on both
// OciObjectStorageBlobStore and OciObjectStorageBlobContainer
// TODO: refactor the code to avoid those duplications. Turn off CPD for now in this section.
// tell cpd to start ignoring code - CPD-OFF
@SuppressWarnings("CPD-START")
@Log4j2
class OciObjectStorageBlobStore implements BlobStore {
    private static final int NOT_FOUND = 404;

    // The recommended maximum size of a blob that should be uploaded in a single
    // request. Larger files should be uploaded over multiple requests
    public static final long MULTIPART_BUFFER_SIZE;

    private static final RetryConfiguration RETRY_CONFIGURATION;

    static {
        final String key = "os.repository_oci.buffer_size";
        final String largeBlobThresholdByteSizeProperty = System.getProperty(key);
        if (largeBlobThresholdByteSizeProperty == null) {
            MULTIPART_BUFFER_SIZE = new ByteSizeValue(100, ByteSizeUnit.MB).getBytes();
        } else {
            final long largeBlobThresholdByteSize;
            try {
                largeBlobThresholdByteSize = Long.parseLong(largeBlobThresholdByteSizeProperty);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException(
                        "failed to parse "
                                + key
                                + " having value ["
                                + largeBlobThresholdByteSizeProperty
                                + "]");
            }
            if (largeBlobThresholdByteSize <= 0) {
                throw new IllegalArgumentException(
                        key
                                + " must be positive but was ["
                                + largeBlobThresholdByteSizeProperty
                                + "]");
            }
            MULTIPART_BUFFER_SIZE = largeBlobThresholdByteSize;
        }
        // Initialize RETRY_CONFIGURATION
        RETRY_CONFIGURATION =
                new RetryConfiguration.Builder()
                        .terminationStrategy(
                                new MaxAttemptsTerminationStrategy(
                                        RetryConfiguration.DEFAULT_MAX_RETRY_ATTEMPTS))
                        .delayStrategy(
                                new ExponentialBackoffDelayStrategy(
                                        RetryConfiguration.DEFAULT_MAX_WAIT_TIME))
                        .build();
    }

    private final String repositoryName;
    private final String bucketName;
    private final String namespace;
    private final OciObjectStorageService storageService;

    OciObjectStorageBlobStore(
            final OciObjectStorageService storageService, final RepositoryMetadata metadata) {
        this.storageService = storageService;
        this.repositoryName = metadata.name();

        Settings settings = metadata.settings();
        this.bucketName = BUCKET_SETTING.get(settings);
        this.namespace = NAMESPACE_SETTING.get(settings);
        final String bucketCompartmentId = BUCKET_COMPARTMENT_ID_SETTING.get(settings);
        final boolean forceBucketCreation = FORCE_BUCKET_CREATION_SETTING.get(settings);
        if (!doesBucketExist(namespace, bucketName)) {

            if (forceBucketCreation) {
                log.warn(
                        "Bucket does not exist and force bucket creation is checked, will attempt"
                                + " to force create bucket");
                createBucket(bucketCompartmentId, namespace, bucketName);
            } else {
                throw new BlobStoreException("Bucket [" + bucketName + "] does not exist");
            }
        }
    }

    private ObjectStorageClientReference clientReference() throws IOException {
        return storageService.client(repositoryName);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new OciObjectStorageBlobContainer(path, this);
    }

    @Override
    public void close() throws IOException {
        storageService.releaseClient(repositoryName);
    }

    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists
     */
    private boolean doesBucketExist(String namespace, String bucketName) {
        log.debug("Attempting to check if bucket {} exists in object store", bucketName);
        try (ObjectStorageClientReference clientRef = clientReference()) {

            final GetBucketResponse getBucketResponse =
                    clientRef
                            .get()
                            .getBucket(
                                    GetBucketRequest.builder()
                                            .bucketName(bucketName)
                                            .namespaceName(namespace)
                                            .retryConfiguration(RETRY_CONFIGURATION)
                                            .build());

            return getBucketResponse.getBucket() != null;
        } catch (final Exception e) {
            log.error("doesBucketExist : ", e);
            if (e instanceof BmcException) {
                final BmcException bmcEx = (BmcException) e;
                if (bmcEx.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                    log.warn("bucket couldn't be found");
                    return false;
                } else {
                    log.error(
                            "Couldn't check if bucket [{}] exists. unrecognized error code in"
                                    + " response",
                            bucketName,
                            e);
                    throw new BlobStoreException(
                            "Unable to check if bucket ["
                                    + bucketName
                                    + "] exists, unrecognized error code in response",
                            e);
                }
            }
            throw new BlobStoreException(
                    "Unable to check if bucket [" + bucketName + "] exists", e);
        }
    }

    private void createBucket(
            final String bucketCompartmentId, final String namespace, final String bucketName) {
        try (ObjectStorageClientReference clientRef = clientReference()) {
            clientRef
                    .get()
                    .createBucket(
                            CreateBucketRequest.builder()
                                    .namespaceName(namespace)
                                    .retryConfiguration(RETRY_CONFIGURATION)
                                    .createBucketDetails(
                                            CreateBucketDetails.builder()
                                                    .compartmentId(bucketCompartmentId)
                                                    .name(bucketName)
                                                    .storageTier(
                                                            CreateBucketDetails.StorageTier
                                                                    .Standard)
                                                    .publicAccessType(
                                                            CreateBucketDetails.PublicAccessType
                                                                    .NoPublicAccess)
                                                    .build())
                                    .build());

        } catch (IOException e) {
            throw new BlobStoreException(
                    "Unable to force create bucket [" + bucketName + "] that doesn't exists", e);
        }
    }

    /**
     * List blobs in the specific bucket under the specified path. The path root is removed.
     *
     * @param path base path of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetadata> listBlobs(String path) throws IOException {
        return listBlobsByPrefix(path, "");
    }

    /**
     * List all blobs in the specific bucket with names prefixed
     *
     * @param path base path of the blobs to list. This path is removed from the names of the blobs
     *     returned.
     * @param prefix prefix of the blobs to list.
     * @return a map of blob names and their metadata.
     */
    // TODO: revisit whether this needs to list only immediate files under the directory (like
    // FSBlobContainer) or behave like S3BlobContainer (current behavior) that returns every prefix
    Map<String, BlobMetadata> listBlobsByPrefix(String path, String prefix) throws IOException {
        log.debug("attempting to list blobs by path: {}, prefix: {} ", path, prefix);
        final String pathPrefix = buildKey(path, prefix);
        log.debug("constructed pathPrefix: {} ", pathPrefix);

        final MapBuilder<String, BlobMetadata> mapBuilder = MapBuilder.newMapBuilder();
        try (ObjectStorageClientReference clientRef = clientReference()) {

            try {
                fullListing(clientRef.get(), pathPrefix).stream()
                        .forEach(
                                summary -> {
                                    String suffixName = summary.getName().substring(path.length());
                                    final long size =
                                            summary.getSize() != null ? summary.getSize() : 0;
                                    PlainBlobMetadata metadata =
                                            new PlainBlobMetadata(suffixName, size);
                                    mapBuilder.put(suffixName, metadata);
                                });
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        return mapBuilder.immutableMap();
    }

    Map<String, BlobContainer> listChildren(BlobPath path) throws IOException {
        final String pathStr = path.buildAsString();
        log.debug("attempting to list children by path: {} ", path);

        final MapBuilder<String, BlobContainer> mapBuilder = MapBuilder.newMapBuilder();
        try (ObjectStorageClientReference clientRef = clientReference()) {
            try {
                fullListing(clientRef.get(), pathStr).stream()
                        .map(
                                objectSummary ->
                                        objectSummary
                                                .getName()
                                                .substring(pathStr.length())
                                                .split("/"))
                        .filter(name -> name.length > 1 && !name[0].isEmpty())
                        .map(name -> name[0])
                        .distinct()
                        .forEach(
                                childName -> {
                                    mapBuilder.put(
                                            childName,
                                            new OciObjectStorageBlobContainer(
                                                    path.add(childName), this));
                                });
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        return mapBuilder.immutableMap();
    }

    /**
     * Returns an {@link InputStream} for the given blob name
     *
     * @param blobName name of the blob
     * @return the InputStream used to read the blob's content
     */
    InputStream readBlob(String blobName) throws IOException {
        final String opcClientRequestId = createClientRequestId("getObjects");
        final long startTime = System.currentTimeMillis();
        try (ObjectStorageClientReference clientRef = clientReference()) {

            log.debug(
                    "Getting object from '/n/{}/b/{}/o/{}'." + " OPC-REQUEST-ID: {}",
                    namespace,
                    bucketName,
                    blobName,
                    opcClientRequestId);
            DownloadConfiguration downloadConfiguration = DownloadConfiguration.builder().build();
            DownloadManager downloadManager =
                    new DownloadManager(clientRef.get(), downloadConfiguration);
            return downloadManager
                    .getObject(
                            GetObjectRequest.builder()
                                    .bucketName(bucketName)
                                    .namespaceName(namespace)
                                    .objectName(blobName)
                                    .retryConfiguration(RETRY_CONFIGURATION)
                                    .build())
                    .getInputStream();
        }
    }

    /**
     * Returns an {@link InputStream} for the given blob's position and length
     *
     * @param blobName name of the blob
     * @param position starting position to read from
     * @param length length of bytes to read
     * @return the InputStream used to read the blob's content
     */
    InputStream readBlob(String blobName, long position, long length) throws IOException {
        final String opcClientRequestId = createClientRequestId("getObjects");
        final long startTime = System.currentTimeMillis();

        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        }

        try (ObjectStorageClientReference clientRef = clientReference()) {
            DownloadConfiguration downloadConfiguration = DownloadConfiguration.builder().build();
            DownloadManager downloadManager =
                    new DownloadManager(clientRef.get(), downloadConfiguration);
            return downloadManager
                    .getObject(
                            GetObjectRequest.builder()
                                    .bucketName(bucketName)
                                    .namespaceName(namespace)
                                    .objectName(blobName)
                                    .retryConfiguration(RETRY_CONFIGURATION)
                                    .range(new Range(position, position + length - 1))
                                    .build())
                    .getInputStream();
        }
    }

    /**
     * Writes a blob in the specific bucket
     *
     * @param inputStream content of the blob to be written
     * @param blobSize expected size of the blob to be written
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob
     *     already exists
     */
    void writeBlob(
            String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
        // we will always do multipart uploads
        putObject(inputStream, blobSize, blobName, !failIfAlreadyExists);
    }

    /**
     * Deletes the given path and all its children.
     *
     * @param pathStr Name of path to delete
     */
    DeleteResult deleteDirectory(String pathStr) throws IOException {
        final AtomicLong deletedBlobs = new AtomicLong();
        final AtomicLong deletedBytes = new AtomicLong();
        try (ObjectStorageClientReference clientRef = clientReference()) {

            fullListing(clientRef.get(), pathStr).stream()
                    .forEach(
                            objectSummary -> {
                                final String opcClientRequestId =
                                        createClientRequestId("deleteDirectory");
                                final String objectName = objectSummary.getName();
                                final Instant start = Instant.now();

                                final DeleteObjectRequest deleteObjectRequest =
                                        DeleteObjectRequest.builder()
                                                .bucketName(bucketName)
                                                .namespaceName(namespace)
                                                .objectName(objectName)
                                                .retryConfiguration(RETRY_CONFIGURATION)
                                                .opcClientRequestId(opcClientRequestId)
                                                .build();
                                Failsafe.with(
                                                getRetryPolicy(
                                                        objectName,
                                                        opcClientRequestId,
                                                        start.toEpochMilli(),
                                                        "DELETE"))
                                        .run(
                                                () -> {
                                                    try {
                                                        clientRef
                                                                .get()
                                                                .deleteObject(deleteObjectRequest);
                                                        deletedBlobs.incrementAndGet();
                                                    } catch (Exception e) {
                                                        if (e.getCause() instanceof BmcException) {
                                                            final BmcException bmcEx =
                                                                    (BmcException) e.getCause();
                                                            if (bmcEx.getStatusCode()
                                                                    == HttpURLConnection
                                                                            .HTTP_NOT_FOUND) {
                                                                log.warn(
                                                                        "blob couldn't be found to"
                                                                                + " delete, doing"
                                                                                + " nothing");
                                                            } else {
                                                                log.error(
                                                                        "Couldn't delete blob:"
                                                                            + " n:{}/b:{}/o:{}"
                                                                            + " exists."
                                                                            + " unrecognized error"
                                                                            + " code in response",
                                                                        namespace,
                                                                        bucketName,
                                                                        objectName,
                                                                        e);
                                                                throw new BlobStoreException(
                                                                        "Unable to check if blob ["
                                                                                + objectName
                                                                                + "] exists,"
                                                                                + " unrecognized"
                                                                                + " error code in"
                                                                                + " response",
                                                                        e);
                                                            }
                                                        }
                                                        throw new BlobStoreException(
                                                                "Unable to delete if blob ["
                                                                        + objectName
                                                                        + "] exists",
                                                                e);
                                                    }
                                                });
                            });
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        return new DeleteResult(deletedBlobs.get(), deletedBytes.get());
    }

    /**
     * Deletes multiple blobs from the specific bucket using a batch request
     *
     * @param blobNames names of the blobs to delete
     */
    void deleteBlobsIgnoringIfNotExists(Collection<String> blobNames) throws IOException {
        try (ObjectStorageClientReference clientRef = clientReference()) {
            blobNames.stream()
                    .forEach(
                            blobName -> {
                                List<ObjectSummary> objectSummaries;
                                objectSummaries =
                                        clientRef
                                                .get()
                                                .listObjects(
                                                        ListObjectsRequest.builder()
                                                                .bucketName(bucketName)
                                                                .namespaceName(namespace)
                                                                .prefix(blobName)
                                                                .retryConfiguration(
                                                                        RETRY_CONFIGURATION)
                                                                .build())
                                                .getListObjects()
                                                .getObjects();

                                if (objectSummaries.size() > 1) {
                                    log.error("will not delete {}", blobName);
                                    throw new RuntimeException(
                                            "Something is not right, when trying to"
                                                    + " delete objects, we found more"
                                                    + " than one object for the same"
                                                    + " blob");
                                }
                                if (objectSummaries.size() == 1) {
                                    final String opcClientRequestId =
                                            createClientRequestId("DELETE-ignoring-if-exists");
                                    final ObjectSummary objectSummary = objectSummaries.get(0);
                                    final String objectName = objectSummary.getName();
                                    final Instant start = Instant.now();
                                    final DeleteObjectRequest deleteObjectRequest =
                                            DeleteObjectRequest.builder()
                                                    .bucketName(bucketName)
                                                    .namespaceName(namespace)
                                                    .objectName(objectName)
                                                    .retryConfiguration(RETRY_CONFIGURATION)
                                                    .opcClientRequestId(opcClientRequestId)
                                                    .build();
                                    Failsafe.with(
                                                    getRetryPolicy(
                                                            objectName,
                                                            opcClientRequestId,
                                                            start.toEpochMilli(),
                                                            "DELETE-ignoring-if-exists"))
                                            .run(
                                                    () ->
                                                            clientRef
                                                                    .get()
                                                                    .deleteObject(
                                                                            deleteObjectRequest));
                                }
                            });
        }
    }

    boolean blobExists(String blobName) throws IOException {
        try (ObjectStorageClientReference clientRef = clientReference()) {
            clientRef
                    .get()
                    .headObject(
                            HeadObjectRequest.builder()
                                    .namespaceName(namespace)
                                    .bucketName(bucketName)
                                    .objectName(blobName)
                                    .retryConfiguration(RETRY_CONFIGURATION)
                                    .build());
        } catch (Exception e) {

            if (e instanceof BmcException) {
                final BmcException bmcEx = (BmcException) e;
                if (bmcEx.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                    log.warn("blob couldn't be found");
                    return false;
                } else {
                    log.error(
                            "Couldn't check if blob: n:{}/b:{}/o:{} exists. unrecognized error code"
                                    + " in response",
                            namespace,
                            bucketName,
                            blobName,
                            e);
                    throw new BlobStoreException(
                            "Unable to check if blob ["
                                    + blobName
                                    + "] exists, unrecognized error code in response",
                            e);
                }
            }
            throw new BlobStoreException("Unable to check if blob [" + blobName + "] exists", e);
        }

        return true;
    }

    private static String buildKey(String keyPath, String prefix) {
        assert prefix != null;
        return keyPath + prefix;
    }

    private List<ObjectSummary> fullListing(ObjectStorageClient objectStorageClient, String prefix)
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<ObjectSummary> items = new ArrayList<>();
        String start = null;

        log.debug("Listing {}", prefix);

        do {
            final String startPrefix = start;
            final String requestId = createClientRequestId("listObjects");
            final ListObjectsResponse response =
                    Failsafe.with(
                                    getRetryPolicy(
                                            prefix,
                                            requestId,
                                            Instant.now().toEpochMilli(),
                                            "LIST"))
                            .get(
                                    () ->
                                            objectStorageClient.listObjects(
                                                    ListObjectsRequest.builder()
                                                            .bucketName(bucketName)
                                                            .namespaceName(namespace)
                                                            .prefix(prefix)
                                                            .limit(1000)
                                                            .start(startPrefix)
                                                            .opcClientRequestId(requestId)
                                                            .retryConfiguration(RETRY_CONFIGURATION)
                                                            .build()));

            items.addAll(response.getListObjects().getObjects());
            log.debug("items found: {}", response.getListObjects().getObjects());
            start = response.getListObjects().getNextStartWith();
        } while (start != null && !start.isEmpty());

        return items;
    }

    private static String createClientRequestId(final String operation) {
        final String uuid = UUID.randomUUID().toString();
        log.debug("Using request ID {} for {}", uuid, operation);
        return uuid;
    }

    /** Uploads a file to object storage. */
    private void putObject(
            final InputStream inputStream,
            final long blobSize,
            final String objectName,
            final boolean override)
            throws IOException {
        String opcClientRequestId = createClientRequestId("put");
        try (ObjectStorageClientReference clientRef = clientReference()) {
            final Instant start = Instant.now();

            final UploadConfiguration uploadConfiguration =
                    UploadConfiguration.builder()
                            .allowMultipartUploads(true)
                            .allowParallelUploads(true)
                            .build();
            UploadManager uploadManager = new UploadManager(clientRef.get(), uploadConfiguration);
            final PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .contentLength(blobSize)
                            .bucketName(bucketName)
                            .objectName(objectName)
                            .namespaceName(namespace)
                            .retryConfiguration(RETRY_CONFIGURATION)
                            // For now no retry configured yet we will use fail safe instead
                            // .retryConfiguration(RetryConfiguration.builder()
                            //    .delayStrategy(new ExponentialBackoffDelayStrategy()).build())
                            .opcClientRequestId(opcClientRequestId)
                            .putObjectBody(inputStream)
                            .ifNoneMatch(override ? null : "*")
                            .build();

            Failsafe.with(
                            getRetryPolicy(
                                    objectName, opcClientRequestId, start.toEpochMilli(), "PUT"))
                    .run(
                            () -> {
                                try {
                                    log.debug(
                                            "Pushing object to '/n/{}/b/{}/o/{}'. OPC-REQUEST-ID:"
                                                    + " {}",
                                            namespace,
                                            bucketName,
                                            objectName,
                                            opcClientRequestId);

                                    UploadManager.UploadRequest uploadRequest =
                                            UploadManager.UploadRequest.builder(
                                                            inputStream, blobSize)
                                                    .allowOverwrite(true)
                                                    .build(putObjectRequest);

                                    uploadManager.upload(uploadRequest);

                                    final Instant end = Instant.now();
                                    log.debug(
                                            "Finished pushing object '/n/{}/b/{}/o/{}' in {}"
                                                    + " millis. OPC-REQUEST-ID: {}",
                                            namespace,
                                            bucketName,
                                            objectName,
                                            end.toEpochMilli() - start.toEpochMilli(),
                                            opcClientRequestId);
                                } catch (Throwable e) {
                                    log.error(
                                            "Failed pushing object '/n/{}/b/{}/o/{}."
                                                    + " OPC-REQUEST-ID: {}' ",
                                            namespace,
                                            bucketName,
                                            objectName,
                                            opcClientRequestId,
                                            e);
                                    throw e;
                                }
                            });
        }
    }

    /* The retryable Http error codes are listed in the below link:
     *  - https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm
     * */
    private boolean isRetryable(Throwable error) {
        Optional<BmcException> optionalBmcEx = unwrap(error);
        if (optionalBmcEx.isPresent()) {
            BmcException ex = optionalBmcEx.get();
            Throwable rootCause = findRootCause(error);
            if (ex.isTimeout()
                    || rootCause instanceof java.net.SocketException
                    || rootCause instanceof java.lang.IllegalStateException) {
                // Exceptions that if retried may succeed
                return true;
            } else {
                int httpCode = ex.getStatusCode();
                String errorCode = ex.getServiceCode();
                switch (httpCode) {
                    case 409:
                        if (errorCode.equals("ExternalServerIncorrectState")
                                || errorCode.equals("IncorrectState")) return true;
                        return false;
                    case 503:
                        if (errorCode.equals("ExternalServerUnreachable")
                                || errorCode.equals("ExternalServerTimeout")
                                || errorCode.equals("ExternalServerInvalidResponse")
                                || errorCode.equals("ServiceUnavailable")) return true;
                        return false;
                    case 429:
                    case 500:
                        return true;
                    default:
                        return false;
                }
            }
        }
        log.error("Getting a non BmcException error", error);
        return false;
    }

    private Throwable findRootCause(Throwable error) {
        Throwable rootCause = error;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    private RetryPolicy<Object> getRetryPolicy(
            String objectName, String opcClientRequestId, long start, String op) {
        return new RetryPolicy<>()
                .withJitter(0.2)
                .withBackoff(1, 30, ChronoUnit.SECONDS)
                .withMaxRetries(10)
                .handleIf(this::isRetryable)
                .onRetry(
                        (res) -> {
                            final Optional<BmcException> bmce = unwrap(res.getLastFailure());
                            String responseOpsRequestId =
                                    bmce.map(BmcException::getOpcRequestId)
                                            .orElse(opcClientRequestId);
                            if (bmce.isPresent()) {
                                // OCI storage Client connection may be more error prone and
                                // therefore release it
                                storageService.releaseClient(repositoryName);
                            }
                            log.warn(
                                    "Retrying to {} object from '/n/{}/b/{}/o/{}'. OPC-REQUEST-ID:"
                                            + " {}. Attempt: {}... {}",
                                    op,
                                    namespace,
                                    bucketName,
                                    objectName,
                                    responseOpsRequestId,
                                    res.getAttemptCount() + 1,
                                    getStackTrace(res.getLastFailure()));
                            // TODO: wire telemetry
                            // scope.emit("retry", 1);
                        })
                .onFailure(
                        e -> {
                            String responseOpsRequestId =
                                    unwrap(e.getFailure())
                                            .map(BmcException::getOpcRequestId)
                                            .orElse(opcClientRequestId);
                            log.error(
                                    "{} object '/n/{}/b/{}/o/{}' failed or retries exhausted in {}"
                                            + " millis. OPC-REQUEST-ID: {}, Failing...\n"
                                            + "{}",
                                    op,
                                    namespace,
                                    bucketName,
                                    objectName,
                                    System.currentTimeMillis() - start,
                                    responseOpsRequestId,
                                    getStackTrace(e.getFailure()));
                            // TODO: wire telemetry
                            // scope.emit("failure", 1);
                        });
    }

    public static Optional<BmcException> unwrap(Throwable from) {
        if (from == null) {
            return Optional.empty();
        }
        if (from instanceof BmcException) {
            return Optional.of((BmcException) from);
        } else {
            Throwable throwable = from;

            do {
                if (throwable.getCause() == null) {
                    return Optional.empty();
                }

                throwable = throwable.getCause();
            } while (!throwable.getClass().equals(BmcException.class));

            return Optional.of((BmcException) throwable);
        }
    }
}
