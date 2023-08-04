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

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorageAsync;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.model.ObjectSummary;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.responses.CreateBucketResponse;
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse;
import com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.HeadObjectResponse;
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.responses.AsyncHandler;
import lombok.extern.log4j.Log4j2;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

// Lots of code duplication with OciObjectStorageBlobStore and OciObjectStorageBlobContainer
// Re-using the code will require significant refactoring which is needed on both
// OciObjectStorageBlobStore and OciObjectStorageBlobContainer
// TODO: refactor the code to avoid those duplications. Turn off CPD for now in this section.
// tell cpd to start ignoring code - CPD-OFF
@SuppressWarnings("CPD-START")
@Log4j2
class OciObjectStorageBlobStore implements BlobStore {

    private static final int BAD_REQUEST = 400;
    private static final int NOT_AUTHENTICATED = 401;
    private static final int NOT_FOUND = 404;
    private static final int THROTTLING_ERROR_CODE = 429;
    private static final int INTERNAL_SERVER_ERROR_CODE = 500;
    private static final int CLIENT_SIDE_ERROR_CODE = 0;

    private static final String NOT_AUTHORIZED_OR_NOT_FOUND = "NotAuthorizedOrNotFound";
    private static final String BUCKET_NOT_FOUND = "BucketNotFound";
    private static final String RELATED_RESOURCE_NOT_AUTHORIZED_OR_NOT_FOUND =
            "RelatedResourceNotAuthorizedOrNotFound";

    // The recommended maximum size of a blob that should be uploaded in a single
    // request. Larger files should be uploaded over multiple requests
    public static final int LARGE_BLOB_THRESHOLD_BYTE_SIZE;

    static {
        final String key = "es.repository_oci.large_blob_threshold_byte_size";
        final String largeBlobThresholdByteSizeProperty = System.getProperty(key);
        if (largeBlobThresholdByteSizeProperty == null) {
            LARGE_BLOB_THRESHOLD_BYTE_SIZE =
                    Math.toIntExact(new ByteSizeValue(5, ByteSizeUnit.MB).getBytes());
        } else {
            final int largeBlobThresholdByteSize;
            try {
                largeBlobThresholdByteSize = Integer.parseInt(largeBlobThresholdByteSizeProperty);
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
            LARGE_BLOB_THRESHOLD_BYTE_SIZE = largeBlobThresholdByteSize;
        }
    }

    private final String bucketName;
    private final String namespace;
    private final String clientName;
    private final OciObjectStorageService storageService;
    private final OciObjectStorageClientSettings clientSettings;

    OciObjectStorageBlobStore(
            final String bucketName,
            final String namespace,
            final String clientName,
            final String bucketCompartmentId,
            final boolean forceBucketCreation,
            final OciObjectStorageService storageService,
            final OciObjectStorageClientSettings clientSettings) {
        this.bucketName = bucketName;
        this.clientName = clientName;
        this.storageService = storageService;
        this.namespace = namespace;
        this.clientSettings = clientSettings;
        if (!doesBucketExist(namespace, bucketName)) {

            if (forceBucketCreation) {
                log.info(
                        "Bucket does not exist and force bucket creation is checked, will attempt to force create bucket");
                createBucket(bucketCompartmentId, namespace, bucketName);
            } else {
                throw new BlobStoreException("Bucket [" + bucketName + "] does not exist");
            }
        }
    }

    private ObjectStorageAsync client() throws IOException {
        if (storageService.client(clientName) == null) {
            final Map<String, OciObjectStorageClientSettings> clientSettingsMap = new HashMap<>();
            clientSettingsMap.put(clientName, clientSettings);
            storageService.refreshWithoutClearingCache(clientSettingsMap);
        }

        return storageService.client(clientName);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new OciObjectStorageBlobContainer(path, this);
    }

    @Override
    public void close() throws IOException {
        storageService.close();
    }

    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists
     */
    private boolean doesBucketExist(String namespace, String bucketName) {
        try {
            log.debug("Attempting to check if bucket exists in object store");
            final GetBucketResponse getBucketResponse =
                    client().getBucket(
                            GetBucketRequest.builder()
                                    .bucketName(bucketName)
                                    .namespaceName(namespace)
                                    .build(), new AsyncHandler<>() {
                                @Override
                                public void onSuccess(GetBucketRequest getBucketRequest, GetBucketResponse getBucketResponse) {
                                    log.debug("successfully getting bucket: {} with namespace: {}", bucketName, namespace);
                                }

                                @Override
                                public void onError(GetBucketRequest getBucketRequest, Throwable error) {
                                    log.error("failure getting bucket: {} with namespace: {}", bucketName, namespace, error);
                                }
                            }).get();

            return getBucketResponse.getBucket() != null;
        } catch (final Exception e) {
            if (e.getCause() instanceof BmcException) {
                final BmcException bmcEx =  (BmcException)e.getCause();
                if (bmcEx.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                    log.info("bucket couldn't be found");
                    return false;
                } else {
                    log.error(
                            "Couldn't check if bucket [{}] exists. unrecognized error code in response",
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
        try {
            SocketAccess.doPrivilegedIOException(
                    () ->
                            client().createBucket(
                                    CreateBucketRequest.builder()
                                            .namespaceName(namespace)
                                            .createBucketDetails(
                                                    CreateBucketDetails.builder()
                                                            .compartmentId(
                                                                    bucketCompartmentId)
                                                            .name(bucketName)
                                                            .storageTier(
                                                                    CreateBucketDetails
                                                                            .StorageTier
                                                                            .Standard)
                                                            .publicAccessType(
                                                                    CreateBucketDetails
                                                                            .PublicAccessType
                                                                            .NoPublicAccess)
                                                            .build())
                                            .build(), new AsyncHandler<>() {
                                        @Override
                                        public void onSuccess(CreateBucketRequest createBucketRequest, CreateBucketResponse createBucketResponse) {
                                            log.debug("successfully creating bucket: {} with namespace: {}", bucketName, namespace);
                                        }

                                        @Override
                                        public void onError(CreateBucketRequest createBucketRequest, Throwable error) {
                                            log.error("failure creating bucket: {} with namespace: {}", bucketName, namespace, error);
                                        }
                                    }).get(10, TimeUnit.SECONDS));
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
    Map<String, BlobMetadata> listBlobsByPrefix(String path, String prefix) throws IOException {
        log.info("attempting to list blobs by path: {}, prefix: {} ", path, prefix);
        final String pathPrefix = buildKey(path, prefix);
        log.info("constructed pathPrefix: {} ", pathPrefix);

        final MapBuilder<String, BlobMetadata> mapBuilder = MapBuilder.newMapBuilder();
        SocketAccess.doPrivilegedVoidIOException(
                () ->
                {
                    try {
                        fullListing(client(), pathPrefix).stream()
                                .forEach(
                                        summary -> {
                                            String suffixName =
                                                    summary.getName().substring(path.length());
                                            final long size =
                                                    summary.getSize() != null
                                                            ? summary.getSize()
                                                            : 0;
                                            PlainBlobMetadata metadata =
                                                    new PlainBlobMetadata(suffixName, size);
                                            mapBuilder.put(suffixName, metadata);
                                        });
                    } catch (ExecutionException | InterruptedException | TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                });

        return mapBuilder.immutableMap();
    }

    Map<String, BlobContainer> listChildren(BlobPath path) throws IOException {
        final String pathStr = path.buildAsString();
        log.info("attempting to list children by path: {} ", path);

        final MapBuilder<String, BlobContainer> mapBuilder = MapBuilder.newMapBuilder();
        SocketAccess.doPrivilegedVoidIOException(
                () ->
                {
                    try {
                        fullListing(client(), pathStr).stream()
                                .map(
                                        objectSummary ->
                                                objectSummary
                                                        .getName()
                                                        .substring(pathStr.length())
                                                        .split("/"))
                                .filter(name -> name.length > 0 && !name[0].isEmpty())
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
                });
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
        return Failsafe.with(getRetryPolicy(blobName, opcClientRequestId, startTime, "GET"))
                .get(
                        () ->
                                SocketAccess.doPrivilegedIOException(
                                        () -> {
                                            log.info(
                                                    "Getting object from '/n/{}/b/{}/o/{}'. OPC-REQUEST-ID: {}",
                                                    namespace,
                                                    bucketName,
                                                    blobName,
                                                    opcClientRequestId);
                                            return client().getObject(
                                                            GetObjectRequest.builder()
                                                                    .bucketName(bucketName)
                                                                    .namespaceName(namespace)
                                                                    .objectName(blobName)
                                                                    .build(), new AsyncHandler<>() {
                                                        @Override
                                                        public void onSuccess(GetObjectRequest getObjectRequest, GetObjectResponse getObjectResponse) {
                                                            log.debug(
                                                                    "Success getting object from '/n/{}/b/{}/o/{}'. OPC-REQUEST-ID: {}",
                                                                    namespace,
                                                                    bucketName,
                                                                    blobName,
                                                                    opcClientRequestId);
                                                        }

                                                        @Override
                                                        public void onError(GetObjectRequest getObjectRequest, Throwable error) {
                                                            log.debug(
                                                                    "Failure getting object from '/n/{}/b/{}/o/{}'. OPC-REQUEST-ID: {}",
                                                                    namespace,
                                                                    bucketName,
                                                                    blobName,
                                                                    opcClientRequestId, error);
                                                        }
                                                    }).get().getInputStream();
                                        }));
    }

    /**
     * Returns an {@link InputStream} for the given blob's position and length
     *
     * @param blobName name of the blob
     * @param position starting position to read from
     * @param length length of bytes to read
     * @return the InputStream used to read the blob's content
     */
    InputStream readBlob(String blobName, long position, long length) {
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
        } else {
            return Failsafe.with(getRetryPolicy(blobName, opcClientRequestId, startTime, "GET"))
                    .get( () ->
                            client().getObject(
                            GetObjectRequest.builder()
                                    .bucketName(bucketName)
                                    .namespaceName(namespace)
                                    .objectName(blobName)
                                    .range(new Range(position, position + length -1))
                                    .build(), new AsyncHandler<>() {
                                @Override
                                public void onSuccess(GetObjectRequest getObjectRequest, GetObjectResponse getObjectResponse) {
                                    log.debug(
                                            "Success getting object from '/n/{}/b/{}/o/{}'",
                                            namespace,
                                            bucketName,
                                            blobName);
                                }

                                @Override
                                public void onError(GetObjectRequest getObjectRequest, Throwable error) {
                                    log.debug(
                                            "Failure getting object from '/n/{}/b/{}/o/{}'",
                                            namespace,
                                            bucketName,
                                            blobName,
                                            error);
                                }
                            }).get()
                    .getInputStream());
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
        SocketAccess.doPrivilegedVoidIOException(
                () -> putObject(inputStream, blobSize, blobName, !failIfAlreadyExists));
    }

    /**
     * Deletes the given path and all its children.
     *
     * @param pathStr Name of path to delete
     */
    DeleteResult deleteDirectory(String pathStr) throws IOException {
        final AtomicLong deletedBlobs = new AtomicLong();
        final AtomicLong deletedBytes = new AtomicLong();
        final ObjectStorageAsync client = client();

        try {
            fullListing(client, pathStr).stream()
                    .forEach(
                            objectSummary -> {
                                final String opcClientRequestId = createClientRequestId("deleteDirectory");
                                final String objectName = objectSummary.getName();
                                final Instant start = Instant.now();

                                final DeleteObjectRequest deleteObjectRequest =
                                        DeleteObjectRequest.builder()
                                                .bucketName(bucketName)
                                                .namespaceName(namespace)
                                                .objectName(objectName)
                                                .opcClientRequestId(opcClientRequestId)
                                                .build();
                                Failsafe.with(getRetryPolicy(
                                                objectName, opcClientRequestId, start.toEpochMilli(), "DELETE"))
                                        .run(() ->  {
                                            client.deleteObject(deleteObjectRequest, new AsyncHandler<>() {
                                                @Override
                                                public void onSuccess(DeleteObjectRequest deleteObjectRequest, DeleteObjectResponse deleteObjectResponse) {
                                                    log.info("received success for delete");
                                                }

                                                @Override
                                                public void onError(DeleteObjectRequest deleteObjectRequest, Throwable error) {
                                                    log.info("received error for delete");
                                                }
                                            }).get(10, TimeUnit.SECONDS);
                                            deletedBlobs.incrementAndGet();
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
        ObjectStorageAsync client = client();
        SocketAccess.doPrivilegedVoidIOException(
                () -> blobNames.stream()
                        .forEach(
                                blobName -> {
                                    List<ObjectSummary> objectSummaries;
                                    try {
                                        objectSummaries = client.listObjects(
                                                        ListObjectsRequest.builder()
                                                                .bucketName(bucketName)
                                                                .namespaceName(namespace)
                                                                .prefix(blobName)
                                                                .build(), new AsyncHandler<>() {
                                                            @Override
                                                            public void onSuccess(ListObjectsRequest listObjectsRequest, ListObjectsResponse listObjectsResponse) {
                                                                log.debug("successfully listing prefix:{}, bucket: {}, namespace: {}", blobName, bucketName, namespace);
                                                            }

                                                            @Override
                                                            public void onError(ListObjectsRequest listObjectsRequest, Throwable error) {
                                                                log.error("error listing prefix:{}, bucket: {}, namespace: {}", blobName, bucketName, namespace, error);
                                                            }
                                                        }).get(30, TimeUnit.SECONDS)
                                                .getListObjects()
                                                .getObjects();
                                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                        throw new RuntimeException(e);
                                    }

                                    if (objectSummaries.size() > 1) {
                                        log.error("will not delete {}", blobName);
                                        throw new RuntimeException(
                                                "Something is not right, when trying to delete objects, "
                                                        + "we found more than one object for the same blob");
                                    }
                                    if (objectSummaries.size() == 1) {
                                        final String opcClientRequestId = createClientRequestId("DELETE-ignoring-if-exists");
                                        final ObjectSummary objectSummary = objectSummaries.get(0);
                                        final String objectName = objectSummary.getName();
                                        final Instant start = Instant.now();
                                        final DeleteObjectRequest deleteObjectRequest =
                                                DeleteObjectRequest.builder()
                                                        .bucketName(bucketName)
                                                        .namespaceName(namespace)
                                                        .objectName(objectName)
                                                        .opcClientRequestId(opcClientRequestId)
                                                        .build();
                                        Failsafe.with(getRetryPolicy(
                                                objectName, opcClientRequestId, start.toEpochMilli(), "DELETE-ignoring-if-exists"))
                                                        .run(() -> client.deleteObject(deleteObjectRequest, new AsyncHandler<DeleteObjectRequest, DeleteObjectResponse>() {
                                                            @Override
                                                            public void onSuccess(DeleteObjectRequest deleteObjectRequest, DeleteObjectResponse deleteObjectResponse) {

                                                            }

                                                            @Override
                                                            public void onError(DeleteObjectRequest deleteObjectRequest, Throwable error) {

                                                            }
                                                        }));
                                    }
                                }));
    }

    boolean blobExists(String blobName) throws IOException {
        final ObjectStorageAsync client = client();
        try {
            SocketAccess.doPrivilegedVoidIOException(
                    () ->
                    {
                        try {
                            client.headObject(
                                    HeadObjectRequest.builder()
                                            .namespaceName(namespace)
                                            .bucketName(bucketName)
                                            .objectName(blobName)
                                            .build(), new AsyncHandler<HeadObjectRequest, HeadObjectResponse>() {
                                        @Override
                                        public void onSuccess(HeadObjectRequest headObjectRequest, HeadObjectResponse headObjectResponse) {
                                            log.debug("successfully head object: {}, bucket: {}, namespace: {}", blobName, bucketName, namespace);
                                        }

                                        @Override
                                        public void onError(HeadObjectRequest headObjectRequest, Throwable error) {
                                            log.error("failure head object: {}, bucket: {}, namespace: {}", blobName, bucketName, namespace, error);
                                        }
                                    }).get(10, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (BmcException bmce) {
            if (bmce.getStatusCode() == NOT_FOUND) {
                return false;
            }
            throw bmce;
        }

        return true;
    }

    private static String buildKey(String keyPath, String prefix) {
        assert prefix != null;
        return keyPath + prefix;
    }

    private List<ObjectSummary> fullListing(ObjectStorageAsync objectStorageClient, String prefix) throws ExecutionException, InterruptedException, TimeoutException {
        final List<ObjectSummary> items = new ArrayList<>();
        String start = null;

        log.info("Listing {}", prefix);

        do {
            final String startPrefix = start;
            final String requestId = createClientRequestId("listObjects");
            final ListObjectsResponse response = Failsafe.with(getRetryPolicy(prefix, requestId, Instant.now().toEpochMilli(), "LIST"))
                    .get(() -> objectStorageClient.listObjects(
                            ListObjectsRequest.builder()
                                    .bucketName(bucketName)
                                    .namespaceName(namespace)
                                    .prefix(prefix)
                                    .limit(1000)
                                    .start(startPrefix)
                                    .opcClientRequestId(requestId)
                                    .build(), new AsyncHandler<>() {
                                @Override
                                public void onSuccess(ListObjectsRequest listObjectsRequest, ListObjectsResponse listObjectsResponse) {
                                    log.debug("success listing bucket: {}, with namespace: {}", bucketName, namespace);
                                }

                                @Override
                                public void onError(ListObjectsRequest listObjectsRequest, Throwable error) {
                                    log.error("error listing bucket: {}, with namespace: {}", bucketName, namespace, error);
                                }
                            }).get(10, TimeUnit.SECONDS));
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
            final boolean override) {

        try {
            final ObjectStorageAsync client = client();
            final Instant start = Instant.now();
            String opcClientRequestId = createClientRequestId("put");
            final PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .contentLength(blobSize)
                            .bucketName(bucketName)
                            .objectName(objectName)
                            .namespaceName(namespace)
                            // For now no retry configured yet we will use fail safe instead
                            // .retryConfiguration(RetryConfiguration.builder()
                            //    .delayStrategy(new ExponentialBackoffDelayStrategy()).build())
                            .opcClientRequestId(opcClientRequestId)
                            .putObjectBody(inputStream)
                            .ifNoneMatch(override ? null : "*").build();

            Failsafe.with(
                            getRetryPolicy(
                                    objectName, opcClientRequestId, start.toEpochMilli(), "PUT"))
                    .run(
                            () -> {
                                log.info(
                                        "Pushing object to '/n/{}/b/{}/o/{}'. OPC-REQUEST-ID: {}",
                                        namespace,
                                        bucketName,
                                        objectName,
                                        opcClientRequestId);

                                client.putObject(putObjectRequest, new AsyncHandler<>() {
                                    @Override
                                    public void onSuccess(PutObjectRequest putObjectRequest, PutObjectResponse putObjectResponse) {
                                        log.debug("successfully put object: {}, bucket: {}, namespace: {}", objectName, bucketName, namespace);
                                    }

                                    @Override
                                    public void onError(PutObjectRequest putObjectRequest, Throwable error) {
                                        log.error("error put object: {}, bucket: {}, namespace: {}", objectName, bucketName, namespace, error);
                                    }
                                }).get(60, TimeUnit.SECONDS);
                                final Instant end = Instant.now();
                                log.info(
                                        "Finished pushing object '/n/{}/b/{}/o/{}' in {} millis",
                                        namespace,
                                        bucketName,
                                        objectName,
                                        end.toEpochMilli() - start.toEpochMilli());
                            });

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RetryPolicy<Object> getRetryPolicy(
            String objectName, String opcClientRequestId, long start, String op) {
        return new RetryPolicy<>()
                .withJitter(0.2)
                .withBackoff(100, 10000, ChronoUnit.MILLIS, 5)
                .withMaxRetries(10)
                .onRetry(
                        (res) -> {
                            client().refreshClient();
                            String responseOpsRequestId =
                                    unwrap(res.getLastFailure())
                                            .map(BmcException::getOpcRequestId)
                                            .orElse(opcClientRequestId);
                            log.warn(
                                    "Retrying to {} object from '/n/{}/b/{}/o/{}'. OPC-REQUEST-ID: {}. Attempt: {}... {}",
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
                                    "{} object '/n/{}/b/{}/o/{}' failed or retries exhausted in {} millis. OPC-REQUEST-ID: {}, Failing...\n{}",
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
