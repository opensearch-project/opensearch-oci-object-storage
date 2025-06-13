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
import java.util.Collection;
import java.util.Map;
import org.opensearch.common.blobstore.*;

public interface OciBlobStore extends BlobStore {
    Map<String, BlobMetadata> listBlobs(String path) throws IOException;

    // TODO: revisit whether this needs to list only immediate files under the directory (like
    // FSBlobContainer) or behave like S3BlobContainer (current behavior) that returns every prefix
    Map<String, BlobMetadata> listBlobsByPrefix(String path, String prefix) throws IOException;

    Map<String, BlobContainer> listChildren(BlobPath path) throws IOException;

    InputStream readBlob(String blobName) throws IOException;

    InputStream readBlob(String blobName, long position, long length) throws IOException;

    void writeBlob(
            String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException;

    DeleteResult deleteDirectory(String pathStr) throws IOException;

    void deleteBlobsIgnoringIfNotExists(Collection<String> blobNames) throws IOException;

    boolean blobExists(String blobName) throws IOException;
}
