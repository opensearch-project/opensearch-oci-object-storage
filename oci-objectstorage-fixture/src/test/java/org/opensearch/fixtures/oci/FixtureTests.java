package org.opensearch.fixtures.oci;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;
import com.oracle.bmc.http.client.jersey.ApacheClientProperties;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.responses.*;
import lombok.extern.log4j.Log4j2;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.common.io.Streams;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

@Log4j2
public class FixtureTests {
    private static final String NAMESPACE = "myNamespace";
    private static final String BUCKET_NAME = "myBucket";
    private static final String COMPARTMENT_ID = "myCompartmentId";

    private final BasicAuthenticationDetailsProvider authenticationDetailsProvider =
            SimpleAuthenticationDetailsProvider.builder()
                    .userId("userId")
                    .tenantId("tenantId")
                    .region(Region.US_ASHBURN_1)
                    .fingerprint("fingerprint")
                    .privateKeySupplier(
                            () -> {
                                try {
                                    return new FileInputStream("src/test/resources/fakeKey.pem");
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .build();

    private ObjectStorage objectStorage;

    private NonJerseyServer nonJerseyServer;

    private WebTarget target;

    @Before
    public void setup() throws Exception {
        // start the server
        nonJerseyServer = new NonJerseyServer();
        nonJerseyServer.start();

        // create the client
        Client c = ClientBuilder.newClient();

        target = c.target(NonJerseyServer.DEFAULT_BASE_URI);
        objectStorage =
                ObjectStorageClient.builder()
                    // This will run after, and in addition to, the default
                    // client configurator;
                    // this allows you to get the default behavior from the
                    // default client
                    // configurator
                    // (in the case of the ObjectStorageClient, the
                    // non-buffering behavior), but
                    // you
                    // can also add other things on top of it, like adding new
                    // headers

                    .additionalClientConfigurator(
                        builder -> {
                            // Define a connection manager and its
                            // properties
                            final PoolingHttpClientConnectionManager
                                poolConnectionManager =
                                new PoolingHttpClientConnectionManager();
                            // Setting intentionally pool to 1 to find connections leaks
                            poolConnectionManager.setMaxTotal(1);
                            poolConnectionManager.setDefaultMaxPerRoute(1);

                            builder.property(
                                ApacheClientProperties
                                    .CONNECTION_MANAGER,
                                poolConnectionManager);
                        })
                        .endpoint("http://localhost:8080")
                        .build(authenticationDetailsProvider);
    }

    @After
    public void tearDown() throws Exception {
        // server.stop();
        objectStorage.close();
        nonJerseyServer.close();
    }

    /** Test to see that the message "Got it!" is sent in the response. */
    @Test
    public void testResource() {
        String responseMsg = target.path("/n/testResource").request().get(String.class);
        assertEquals("Got it!", responseMsg);
    }

    @Test
    public void testEachApiOnce() throws IOException {
        // 1. Create bucket
        final CreateBucketResponse createBucketResponse =
            objectStorage.createBucket(
                CreateBucketRequest.builder()
                    .namespaceName(NAMESPACE)
                    .createBucketDetails(
                        CreateBucketDetails.builder()
                            .compartmentId(COMPARTMENT_ID)
                            .metadata(new HashMap<>())
                            .name(BUCKET_NAME)
                            .build())
                    .build());
        assertEquals(BUCKET_NAME, createBucketResponse.getBucket().getName());

        runObjectApis();
    }

    @Test(timeout = 30_000)
    public void testConnectionLeak() throws IOException {
        // 1. Create bucket
        final CreateBucketResponse createBucketResponse =
            objectStorage.createBucket(
                CreateBucketRequest.builder()
                    .namespaceName(NAMESPACE)
                    .createBucketDetails(
                        CreateBucketDetails.builder()
                            .compartmentId(COMPARTMENT_ID)
                            .metadata(new HashMap<>())
                            .name(BUCKET_NAME)
                            .build())
                    .build());
        assertEquals(BUCKET_NAME, createBucketResponse.getBucket().getName());

        // ObjectStorageClient in this test should be configured with connection pool of size 1.
        // So on second method invocation pool will run out of connection and execution wil get stuck.
        // Timeout set for this method will spot that method did not finished by itself.
        // Timeout value is set generous to run well even on the slowest machine.
        for (int i=0;i<2;i++) {
            runObjectApis();
        }
    }

    private void runObjectApis() throws IOException {

        // 2. Create object
        final PutObjectResponse putObjectResponse =
                objectStorage.putObject(
                        PutObjectRequest.builder()
                                .namespaceName(NAMESPACE)
                                .bucketName(BUCKET_NAME)
                                .objectName("/myPrefix/myObject")
                                .putObjectBody(
                                        new ByteArrayInputStream(
                                                "myContent".getBytes(StandardCharsets.UTF_8)))
                                .build());

        assertEquals(200, putObjectResponse.get__httpStatusCode__());
        log.info("putObjectResponse: {}", putObjectResponse);

        // 3. List object
        final ListObjectsResponse listObjectsResponse =
                objectStorage.listObjects(
                        ListObjectsRequest.builder()
                                .bucketName(BUCKET_NAME)
                                .namespaceName(NAMESPACE)
                                .prefix("myPrefix")
                                .build());
        log.info("listObjectsResponse: {}", listObjectsResponse);
        assertEquals(1, listObjectsResponse.getListObjects().getObjects().size());
        assertEquals(
                "/myPrefix/myObject",
                listObjectsResponse.getListObjects().getObjects().get(0).getName());

        // 4. Get object
        final GetObjectResponse getObjectResponse =
                objectStorage.getObject(
                        GetObjectRequest.builder()
                                .namespaceName(NAMESPACE)
                                .bucketName(BUCKET_NAME)
                                .objectName("/myPrefix/myObject")
                                .build());
        log.info("getObjectResponse: {}", getObjectResponse);
        assertEquals(
                "myContent", Streams.readFully(getObjectResponse.getInputStream()).utf8ToString());

        // 4.1
        final GetObjectResponse getObjectResponseWithRange =
                objectStorage.getObject(
                        GetObjectRequest.builder()
                                .namespaceName(NAMESPACE)
                                .bucketName(BUCKET_NAME)
                                .range(new Range(0L, 1L))
                                .objectName("/myPrefix/myObject")
                                .build());
        log.info("getObjectResponse: {}", getObjectResponse);
        assertEquals(
                "my", Streams.readFully(getObjectResponseWithRange.getInputStream()).utf8ToString());

        // 5. Delete object
        final DeleteObjectResponse deleteObjectResponse =
                objectStorage.deleteObject(
                        DeleteObjectRequest.builder()
                                .namespaceName(NAMESPACE)
                                .bucketName(BUCKET_NAME)
                                .objectName("/myPrefix/myObject")
                                .build());
        log.info("deleteObjectResponse: {}", deleteObjectResponse);

        assertEquals(200, deleteObjectResponse.get__httpStatusCode__());

        // 6. List object after delete
        final ListObjectsResponse listObjectsResponse2 =
                objectStorage.listObjects(
                        ListObjectsRequest.builder()
                                .bucketName(BUCKET_NAME)
                                .namespaceName(NAMESPACE)
                                .prefix("myPrefix")
                                .build());
        log.info("listObjectsResponse: {}", listObjectsResponse2);
        assertEquals(0, listObjectsResponse2.getListObjects().getObjects().size());
    }
}
