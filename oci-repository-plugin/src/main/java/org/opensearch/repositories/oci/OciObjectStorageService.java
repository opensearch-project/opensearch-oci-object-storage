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

import static java.util.Collections.emptyMap;

import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.util.LazyInitializable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public class OciObjectStorageService {
    /**
     * Dictionary of client instances. Client instances are built lazily from the latest settings.
     */
    private final AtomicReference<Map<String, LazyInitializable<ObjectStorage, IOException>>>
            clientsCache = new AtomicReference<>(emptyMap());

    /**
     * Refreshes the client settings of existing and new clients. Will not clear the cache of other
     * clients. Subsequent calls to {@code OciObjectStorageService#client} will return new clients
     * constructed using the parameter settings.
     *
     * @param clientsSettings the new settings used for building clients for subsequent requests
     */
    public synchronized void refreshWithoutClearingCache(
            Map<String, OciObjectStorageClientSettings> clientsSettings) {

        // build the new lazy clients
        final Map<String, LazyInitializable<ObjectStorage, IOException>> oldClientCache =
                clientsCache.get();
        final MapBuilder<String, LazyInitializable<ObjectStorage, IOException>> newClientsCache =
                MapBuilder.newMapBuilder();

        // replace or add new clients
        newClientsCache.putAll(oldClientCache);
        for (final Map.Entry<String, OciObjectStorageClientSettings> entry :
                clientsSettings.entrySet()) {
            final LazyInitializable<ObjectStorage, IOException> previousClient =
                    oldClientCache.get(entry.getKey());
            newClientsCache.put(
                    entry.getKey(),
                    new LazyInitializable<>(() -> createClient(entry.getKey(), entry.getValue())));
            // we will release the previous client for this entry if existed
            if (previousClient != null) {
                previousClient.reset();
            }
        }

        clientsCache.getAndSet(newClientsCache.immutableMap());
    }

    /**
     * Attempts to retrieve a client from the cache. If the client does not exist it will be created
     * from the latest settings and will populate the cache. The returned instance should not be
     * cached by the calling code. Instead, for each use, the (possibly updated) instance should be
     * requested by calling this method.
     *
     * @param clientName name of the client settings used to create the client
     * @return a cached client storage instance that can be used to manage objects (blobs)
     */
    public ObjectStorage client(final String clientName) throws IOException {
        final LazyInitializable<ObjectStorage, IOException> lazyClient =
                clientsCache.get().get(clientName);
        if (lazyClient == null) {
            log.warn("No client found for client name");
            return null;
        }

        return lazyClient.getOrCompute();
    }

    /**
     * Creates a client that can be used to manage OCI Object Storage objects. The client is
     * thread-safe.
     *
     * @param clientName name of client settings to use, including secure settings
     * @param clientSettings name of client settings to use, including secure settings
     * @return a new client storage instance that can be used to manage objects (blobs)
     */
    private ObjectStorage createClient(
            String clientName, OciObjectStorageClientSettings clientSettings) throws IOException {
        log.debug(
                () ->
                        new ParameterizedMessage(
                                "creating OCI object store client with client_name [{}], endpoint [{}]",
                                clientName,
                                clientSettings.getEndpoint()));

        final ObjectStorage objectStorageClient =
                SocketAccess.doPrivilegedIOException(
                        () ->
                                new ObjectStorageClient(
                                        clientSettings.getAuthenticationDetailsProvider()));

        objectStorageClient.setEndpoint(clientSettings.getEndpoint());

        return objectStorageClient;
    }
}
