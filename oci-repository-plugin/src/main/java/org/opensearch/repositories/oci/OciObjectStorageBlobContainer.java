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

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Lots of code duplication with OciObjectStorageBlobStore and OciObjectStorageBlobContainer
// Re-using the code will require significant refactoring which is needed on both
// OciObjectStorageBlobStore and OciObjectStorageBlobContainer
// TODO: refactor the code to avoid those duplications. Turn off CPD for now in this section.
// tell cpd to start ignoring code - CPD-OFF
@SuppressWarnings("CPD-START")
class OciObjectStorageBlobContainer extends AbstractBlobContainer {

    private final OciObjectStorageBlobStore blobStore;
    private final String path;

    OciObjectStorageBlobContainer(BlobPath path, OciObjectStorageBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.path = path.buildAsString();
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return blobStore.listBlobs(path);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return blobStore.listChildren(path());
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String prefix) throws IOException {
        return blobStore.listBlobsByPrefix(path, prefix);
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
        throw new RuntimeException("not implemented");
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
        return path + blobName;
    }
}
