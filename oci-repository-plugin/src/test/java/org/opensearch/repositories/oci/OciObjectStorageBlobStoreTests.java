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

import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageAsync;
import com.oracle.bmc.objectstorage.ObjectStorageAsyncClient;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.requests.CreateBucketRequest;
import com.oracle.bmc.objectstorage.requests.GetBucketRequest;
import com.oracle.bmc.objectstorage.responses.CreateBucketResponse;
import com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import com.oracle.bmc.responses.AsyncHandler;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.fixtures.oci.NonJerseyServer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class OciObjectStorageBlobStoreTests {

    static NonJerseyServer nonJerseyServer;
    @BeforeClass
    public static void setup() throws IOException {
        nonJerseyServer = new NonJerseyServer(8083);
        nonJerseyServer.start();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        nonJerseyServer.close();
    }

    @Test
    public void testBlobStoreList_children() throws NoSuchAlgorithmException, IOException {
        final RepositoryMetadata repositoryMetadata =
                new RepositoryMetadata("myOciRepository",
                        OciObjectStorageRepository.TYPE,
                        TestConstants.getRepositorySettings(nonJerseyServer.getUrl(), "listChildren"));

        final OciObjectStorageBlobStore blobStore =
                new OciObjectStorageBlobStore(new OciObjectStorageService(), repositoryMetadata);

        final BlobPath basePath = BlobPath.cleanPath().add(OciObjectStorageRepository.BASE_PATH_SETTING.get(repositoryMetadata.settings()));
        final BlobContainer rootBlobContainer = blobStore.blobContainer(basePath);

        final String blobData1 = "myBlobData1";
        final String blobData2 = "myBlobData1";
        final String blobData3 = "myBlobData3";
        final byte[] blobBytes1 = blobData1.getBytes(StandardCharsets.UTF_8);
        final byte[] blobBytes2 = blobData2.getBytes(StandardCharsets.UTF_8);
        final byte[] blobBytes3 = blobData3.getBytes(StandardCharsets.UTF_8);

        final String blobName1 = "myNewBlob1";
        final String blobName2 = "myNewBlob2";
        final String blobName3 = "myNewBlob3";

        final BlobPath blobPath1 = BlobPath.cleanPath().add("nested").add(blobName1);
        final BlobPath blobPath2 = BlobPath.cleanPath().add("nested").add("nested").add(blobName2);
        final BlobPath blobPath3 = BlobPath.cleanPath().add("nested").add("nested").add("nested").add(blobName3);

        rootBlobContainer.writeBlobAtomic(blobPath1.buildAsString(), new ByteArrayInputStream(blobBytes1), blobBytes1.length, true);
        rootBlobContainer.writeBlobAtomic(blobPath2.buildAsString(), new ByteArrayInputStream(blobBytes2), blobBytes2.length, true);
        rootBlobContainer.writeBlobAtomic(blobPath3.buildAsString(), new ByteArrayInputStream(blobBytes3), blobBytes3.length, true);

        BlobContainer child = rootBlobContainer;
        Map<String, BlobMetadata> blobsUnder = child.listBlobs();
        Assertions.assertThat(blobsUnder.size()).isEqualTo(3);
        Assertions.assertThat(child.blobExists(blobName1)).isFalse();
        Assertions.assertThat(child.blobExists(blobName2)).isFalse();
        Assertions.assertThat(child.blobExists(blobName3)).isFalse();

        Map<String, BlobContainer> children = child.children();
        Assertions.assertThat(children.size()).isEqualTo(1);
        child = children.get("nested");
        blobsUnder = child.listBlobs();
        Assertions.assertThat(blobsUnder.size()).isEqualTo(3);
        Assertions.assertThat(child.blobExists(blobName1)).isTrue();
        Assertions.assertThat(child.blobExists(blobName2)).isFalse();
        Assertions.assertThat(child.blobExists(blobName3)).isFalse();

        children = child.children();
        Assertions.assertThat(children.size()).isEqualTo(1);
        Assertions.assertThat(children.containsKey("nested")).isTrue();
        child = children.get("nested");
        blobsUnder = child.listBlobs();
        Assertions.assertThat(blobsUnder.size()).isEqualTo(2);
        Assertions.assertThat(child.blobExists(blobName1)).isFalse();
        Assertions.assertThat(child.blobExists(blobName2)).isTrue();
        Assertions.assertThat(child.blobExists(blobName3)).isFalse();

        children = child.children();
        Assertions.assertThat(children.size()).isEqualTo(1);
        Assertions.assertThat(children.containsKey("nested")).isTrue();
        child = children.get("nested");
        blobsUnder = child.listBlobs();
        Assertions.assertThat(blobsUnder.size()).isEqualTo(1);
        Assertions.assertThat(child.blobExists(blobName1)).isFalse();
        Assertions.assertThat(child.blobExists(blobName2)).isFalse();
        Assertions.assertThat(child.blobExists(blobName3)).isTrue();

        children = child.children();
        Assertions.assertThat(children.size()).isEqualTo(0);
    }

    @Test
    public void testBlobStoreRead_readRange() throws NoSuchAlgorithmException, IOException {
        final RepositoryMetadata repositoryMetadata =
                new RepositoryMetadata("myOciRepository",
                        OciObjectStorageRepository.TYPE,
                        TestConstants.getRepositorySettings(nonJerseyServer.getUrl(), "testBlobStoreRead_readRange"));

        final OciObjectStorageBlobStore blobStore =
                new OciObjectStorageBlobStore(new OciObjectStorageService(), repositoryMetadata);

        final BlobPath basePath = BlobPath.cleanPath().add(OciObjectStorageRepository.BASE_PATH_SETTING.get(repositoryMetadata.settings()));
        final BlobContainer rootBlobContainer = blobStore.blobContainer(basePath);

        final String blobData = "myBlobData";
        final byte[] blobBytes = blobData.getBytes(StandardCharsets.UTF_8);
        final String blobName = "myNewBlob";
        rootBlobContainer.writeBlobAtomic(blobName, new ByteArrayInputStream(blobBytes), blobBytes.length, true);

        Assertions.assertThat(rootBlobContainer.blobExists(blobName)).isTrue();
        final byte[] returnedBytes =rootBlobContainer.readBlob(blobName, 0, 2).readAllBytes();
        final String returnedBlobData = new String(returnedBytes, StandardCharsets.UTF_8);
        Assertions.assertThat(returnedBlobData).isEqualTo("my");
    }

    @Test
    public void testBlobStoreDelete() throws IOException, NoSuchAlgorithmException {
        final RepositoryMetadata repositoryMetadata =
                new RepositoryMetadata("myOciRepository",
                        OciObjectStorageRepository.TYPE,
                        TestConstants.getRepositorySettings(nonJerseyServer.getUrl(), "testBlobStoreDelete"));

        final OciObjectStorageBlobStore blobStore =
                new OciObjectStorageBlobStore(new OciObjectStorageService(), repositoryMetadata);

        final BlobPath basePath = BlobPath.cleanPath().add(OciObjectStorageRepository.BASE_PATH_SETTING.get(repositoryMetadata.settings()));
        final BlobContainer rootBlobContainer = blobStore.blobContainer(basePath);

        final String blobData1 = "myBlobData1";
        final String blobData2 = "myBlobData1";
        final byte[] blobBytes1 = blobData1.getBytes(StandardCharsets.UTF_8);
        final byte[] blobBytes2 = blobData2.getBytes(StandardCharsets.UTF_8);
        final String blobName1 = "myNewBlob1";
        final String blobName2 = "nested/myNewBlob2";
        rootBlobContainer.writeBlobAtomic(blobName1, new ByteArrayInputStream(blobBytes1), blobBytes1.length, true);
        rootBlobContainer.writeBlobAtomic(blobName2, new ByteArrayInputStream(blobBytes2), blobBytes2.length, true);

        Assertions.assertThat(rootBlobContainer.blobExists(blobName1)).isTrue();
        Assertions.assertThat(rootBlobContainer.blobExists(blobName2)).isTrue();
        Assertions.assertThat(rootBlobContainer.listBlobs().size()).isEqualTo(2);

        final Map<String, BlobContainer> children = rootBlobContainer.children();
        Assertions.assertThat(children.size()).isEqualTo(1);
        Assertions.assertThat(children.containsKey("nested"));
        children.get("nested").delete();
        Assertions.assertThat(rootBlobContainer.blobExists(blobName1)).isTrue();
        Assertions.assertThat(rootBlobContainer.listBlobs().size()).isEqualTo(1);
        final String returnedBlobData = new String(rootBlobContainer.readBlob(blobName1).readAllBytes(), StandardCharsets.UTF_8);
        Assertions.assertThat(returnedBlobData).isEqualTo(blobData1);
    }

    @Test
    public void testBlobStoreWriteBlob() throws NoSuchAlgorithmException, IOException {
        final RepositoryMetadata repositoryMetadata =
                new RepositoryMetadata("myOciRepository",
                        OciObjectStorageRepository.TYPE,
                        TestConstants.getRepositorySettings(nonJerseyServer.getUrl(), "testBlobStoreWriteBlob"));

        final OciObjectStorageBlobStore blobStore =
                new OciObjectStorageBlobStore(new OciObjectStorageService(), repositoryMetadata);

        final BlobPath basePath = BlobPath.cleanPath().add(OciObjectStorageRepository.BASE_PATH_SETTING.get(repositoryMetadata.settings()));
        final BlobContainer rootBlobContainer = blobStore.blobContainer(basePath);

        final String blobData = "myBlobData";
        final byte[] blobBytes = blobData.getBytes(StandardCharsets.UTF_8);
        final String blobName = "myNewBlob";
        rootBlobContainer.writeBlob(blobName, new ByteArrayInputStream(blobBytes), blobBytes.length, true);

        Assertions.assertThat(rootBlobContainer.blobExists(blobName)).isTrue();
        final String returnedBlobData = new String(rootBlobContainer.readBlob(blobName).readAllBytes(), StandardCharsets.UTF_8);
        Assertions.assertThat(returnedBlobData).isEqualTo(blobData);
    }

    @Test
    public void testBlobWriteBlob_atomic() throws IOException, NoSuchAlgorithmException {
        final RepositoryMetadata repositoryMetadata =
                new RepositoryMetadata("myOciRepository",
                        OciObjectStorageRepository.TYPE,
                        TestConstants.getRepositorySettings(nonJerseyServer.getUrl(), "testBlobWriteBlob_atomic"));

        final OciObjectStorageBlobStore blobStore =
                new OciObjectStorageBlobStore(new OciObjectStorageService(), repositoryMetadata);

        final BlobPath basePath = BlobPath.cleanPath().add(OciObjectStorageRepository.BASE_PATH_SETTING.get(repositoryMetadata.settings()));
        final BlobContainer rootBlobContainer = blobStore.blobContainer(basePath);

        final String blobData = "myBlobData";
        final byte[] blobBytes = blobData.getBytes(StandardCharsets.UTF_8);
        final String blobName = "myNewBlob";
        rootBlobContainer.writeBlobAtomic(blobName, new ByteArrayInputStream(blobBytes), blobBytes.length, true);

        Assertions.assertThat(rootBlobContainer.blobExists(blobName)).isTrue();
        final String returnedBlobData = new String(rootBlobContainer.readBlob(blobName).readAllBytes(), StandardCharsets.UTF_8);
        Assertions.assertThat(returnedBlobData).isEqualTo(blobData);
    }

    @Test
    public void testBlobStoreConstruction_forceBucketCreation()
            throws IOException, NoSuchAlgorithmException {
        final RepositoryMetadata repositoryMetadata =
                new RepositoryMetadata("myOciRepository",
                        OciObjectStorageRepository.TYPE,
                        TestConstants.getRepositorySettings(nonJerseyServer.getUrl(), "forceBucketCreationTest", true));

        final OciObjectStorageBlobStore blobStore =
                new OciObjectStorageBlobStore(new OciObjectStorageService(), repositoryMetadata);

        final BlobPath basePath = BlobPath.cleanPath().add(OciObjectStorageRepository.BASE_PATH_SETTING.get(repositoryMetadata.settings()));
        final BlobContainer rootBlobContainer = blobStore.blobContainer(basePath);

        Map<String, BlobContainer> results = rootBlobContainer.children();
        Assertions.assertThat(results.values().size()).isEqualTo(0);
    }

    @Test
    public void testBlobStoreConstruction_DoNotForceBucketCreation()
            throws IOException, NoSuchAlgorithmException {
        final RepositoryMetadata repositoryMetadata =
                new RepositoryMetadata("myOciRepository",
                        OciObjectStorageRepository.TYPE,
                        TestConstants.getRepositorySettings(nonJerseyServer.getUrl(), "forceBucketCreationTest", false));

        Assertions.assertThatThrownBy(() ->
                new OciObjectStorageBlobStore(new OciObjectStorageService(), repositoryMetadata)).hasMessageContaining("Bucket [forceBucketCreationTest] does not exist");
    }
}
