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

import lombok.extern.log4j.Log4j2;
import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.ObjectStorageClient;

@Log4j2
public class ObjectStorageClientReference extends RefCountedReleasable<ObjectStorageClient> {

    private final OciObjectStorageClientSettings clientSettings;

    public ObjectStorageClientReference(
            OciObjectStorageClientSettings clientSettings, ObjectStorageClient ref) {
        super("OCI_STORAGE_CLIENT", ref, ref::close);
        this.clientSettings = clientSettings;
    }

    @Override
    protected void closeInternal() {
        super.closeInternal();
        log.debug("Closed OCI object storage client: {}", clientSettings);
    }
}
