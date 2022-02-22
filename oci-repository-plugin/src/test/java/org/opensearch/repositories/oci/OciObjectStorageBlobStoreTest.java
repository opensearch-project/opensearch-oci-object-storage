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
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.requests.CreateBucketRequest;
import com.oracle.bmc.objectstorage.requests.GetBucketRequest;
import com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import org.assertj.core.api.Assertions;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobStoreException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class OciObjectStorageBlobStoreTest {

    public void tearDown() {
        Mockito.validateMockitoUsage();
    }

    @Test
    public void testBlobStoreConstruction_forceBucketCreation()
            throws IOException, NoSuchAlgorithmException {
        // Setup
        final ObjectStorage mockOciObjectStorageClient = Mockito.mock(ObjectStorage.class);
        final OciObjectStorageService mockOciObjectStorageService =
                Mockito.mock(OciObjectStorageService.class);
        Mockito.when(mockOciObjectStorageService.client(Mockito.any()))
                .thenAnswer((Answer<ObjectStorage>) invocationOnMock -> mockOciObjectStorageClient);

        Mockito.when(mockOciObjectStorageClient.getBucket(Mockito.any()))
                .thenReturn(GetBucketResponse.builder().build());

        final String providedBucketName = "myTestBucketName";
        final String providedNamespace = "myTestNamespace";
        final String providedClientName = "myTestClientName";
        final String providedBucketCompartmentId = "myTestBucketCompartmentId";
        final OciObjectStorageClientSettings ociObjectStorageClientSettings =
                new OciObjectStorageClientSettings(
                        new RepositoryMetadata(
                                "myRepoMetadata",
                                "repository",
                                TestConstants.getRepositorySettings()));
        final OciObjectStorageBlobStore blobStore =
                new OciObjectStorageBlobStore(
                        providedBucketName,
                        providedNamespace,
                        providedClientName,
                        providedBucketCompartmentId,
                        true,
                        mockOciObjectStorageService, // storageService
                        ociObjectStorageClientSettings // clientSettings
                        );

        // 1. Make sure we verified bucket exists
        final ArgumentCaptor<GetBucketRequest> getBucketRequestArgumentCaptor =
                ArgumentCaptor.forClass(GetBucketRequest.class);
        Mockito.verify(mockOciObjectStorageClient, Mockito.times(1))
                .getBucket(getBucketRequestArgumentCaptor.capture());
        final GetBucketRequest capturedGetBucketRequest =
                getBucketRequestArgumentCaptor.getAllValues().get(0);
        Assertions.assertThat(capturedGetBucketRequest.getNamespaceName())
                .isEqualTo(providedNamespace);
        Assertions.assertThat(capturedGetBucketRequest.getBucketName())
                .isEqualTo(providedBucketName);

        // 2. Make sure bucket was created
        final ArgumentCaptor<CreateBucketRequest> createBucketRequestArgumentCaptor =
                ArgumentCaptor.forClass(CreateBucketRequest.class);
        Mockito.verify(mockOciObjectStorageClient, Mockito.times(1))
                .createBucket(createBucketRequestArgumentCaptor.capture());
        final CreateBucketRequest capturedCreateBucketRequest =
                createBucketRequestArgumentCaptor.getAllValues().get(0);
        Assertions.assertThat(capturedCreateBucketRequest.getNamespaceName())
                .isEqualTo(providedNamespace);
        final CreateBucketDetails capturedCreateBucketDetails =
                capturedCreateBucketRequest.getCreateBucketDetails();
        Assertions.assertThat(capturedCreateBucketDetails.getName()).isEqualTo(providedBucketName);
        Assertions.assertThat(capturedCreateBucketDetails.getCompartmentId())
                .isEqualTo(providedBucketCompartmentId);
        Assertions.assertThat(capturedCreateBucketDetails.getStorageTier())
                .isEqualTo(CreateBucketDetails.StorageTier.Standard);
    }

    @Test
    public void testBlobStoreConstruction_DoNotForceBucketCreation()
            throws IOException, NoSuchAlgorithmException {
        // Setup
        final ObjectStorageClient mockOciObjectStorageClient =
                Mockito.mock(ObjectStorageClient.class);
        final OciObjectStorageService mockOciObjectStorageService =
                Mockito.mock(OciObjectStorageService.class);
        Mockito.when(mockOciObjectStorageService.client(Mockito.any()))
                .thenAnswer(
                        (Answer<ObjectStorageClient>)
                                invocationOnMock -> mockOciObjectStorageClient);
        Mockito.when(mockOciObjectStorageClient.getBucket(Mockito.any()))
                .thenReturn(GetBucketResponse.builder().build());

        final String providedBucketName = "myTestBucketName";
        final String providedNamespace = "myTestNamespace";
        final String providedClientName = "myTestClientName";
        final String providedBucketCompartmentId = "myTestBucketCompartmentId";
        final OciObjectStorageClientSettings ociObjectStorageClientSettings =
                new OciObjectStorageClientSettings(
                        new RepositoryMetadata(
                                "myRepoMetadata",
                                "repository",
                                TestConstants.getRepositorySettings()));
        Assertions.assertThatThrownBy(
                        () ->
                                new OciObjectStorageBlobStore(
                                        providedBucketName,
                                        providedNamespace,
                                        providedClientName,
                                        providedBucketCompartmentId,
                                        false,
                                        mockOciObjectStorageService, // storageService
                                        ociObjectStorageClientSettings // clientSettings
                                        ))
                .isInstanceOf(BlobStoreException.class);

        // 1. Make sure we verified bucket exists
        final ArgumentCaptor<GetBucketRequest> getBucketRequestArgumentCaptor =
                ArgumentCaptor.forClass(GetBucketRequest.class);
        Mockito.verify(mockOciObjectStorageClient, Mockito.times(1))
                .getBucket(getBucketRequestArgumentCaptor.capture());
        final GetBucketRequest capturedGetBucketRequest =
                getBucketRequestArgumentCaptor.getAllValues().get(0);
        Assertions.assertThat(capturedGetBucketRequest.getNamespaceName())
                .isEqualTo(providedNamespace);
        Assertions.assertThat(capturedGetBucketRequest.getBucketName())
                .isEqualTo(providedBucketName);

        // 2. Make sure no bucket was created
        Mockito.verify(mockOciObjectStorageClient, Mockito.times(0)).createBucket(Mockito.any());
    }
}
