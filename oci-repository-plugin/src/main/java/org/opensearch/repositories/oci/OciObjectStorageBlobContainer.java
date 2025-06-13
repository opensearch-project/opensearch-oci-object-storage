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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.common.blobstore.*;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;

class OciObjectStorageBlobContainer extends AbstractBlobContainer {

    private final OciBlobStore blobStore;

    OciObjectStorageBlobContainer(BlobPath path, OciBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return blobStore.listBlobs(path().buildAsString());
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return blobStore.listChildren(path());
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String prefix) throws IOException {
        return blobStore.listBlobsByPrefix(path().buildAsString(), prefix);
    }

    @Override
    public boolean blobExists(String blobName) throws IOException {
        return blobStore.blobExists(buildKey(blobName));
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return blobStore.readBlob(buildKey(blobName));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        return blobStore.readBlob(buildKey(blobName), position, length);
    }

    @Override
    public void writeBlob(
            String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(
            String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return blobStore.deleteDirectory(path().buildAsString());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        blobStore.deleteBlobsIgnoringIfNotExists(
                blobNames.stream().map(this::buildKey).collect(Collectors.toList()));
    }

    private String buildKey(String blobName) {
        assert blobName != null;
        return path().buildAsString() + blobName;
    }
}
