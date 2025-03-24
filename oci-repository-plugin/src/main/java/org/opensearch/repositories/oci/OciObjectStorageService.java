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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.LazyInitializable;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.ClientConfiguration;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.ObjectStorageAsync;
import org.opensearch.repositories.oci.sdk.com.oracle.bmc.objectstorage.ObjectStorageAsyncClient;

/** Service class to hold client instances */
@Log4j2
public class OciObjectStorageService implements Closeable {
    /**
     * Dictionary of client instances. Client instances are built lazily from the latest settings.
     */
    private final AtomicReference<Map<Settings, LazyInitializable<ObjectStorageAsync, IOException>>>
            clientsCache = new AtomicReference<>(emptyMap());

    /**
     * Attempts to retrieve a client from the cache. If the client does not exist it will be created
     * from the latest settings and will populate the cache. The returned instance should not be
     * cached by the calling code. Instead, for each use, the (possibly updated) instance should be
     * requested by calling this method.
     *
     * @param clientName name of the client settings used to create the client
     * @return a cached client storage instance that can be used to manage objects (blobs)
     */
    public synchronized ObjectStorageAsync client(final Settings clientName) throws IOException {

        final LazyInitializable<ObjectStorageAsync, IOException> lazyClient =
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
     * @param clientCacheKey name of client settings to use, including secure settings
     * @param clientSettings name of client settings to use, including secure settings
     * @return a new client storage instance that can be used to manage objects (blobs)
     */
    static ObjectStorageAsync createClientAsync(
            Settings clientCacheKey, OciObjectStorageClientSettings clientSettings)
            throws IOException {
        log.debug(
                () ->
                        new ParameterizedMessage(
                                "creating OCI object store client with client_name [{}], endpoint"
                                        + " [{}]",
                                clientCacheKey,
                                clientSettings.getEndpoint()));

        final ObjectStorageAsync objectStorageClient =
                SocketAccess.doPrivilegedIOException(
                        () ->
                                ObjectStorageAsyncClient.builder()
                                        .configuration(ClientConfiguration.builder().build())
                                        .build(clientSettings.getAuthenticationDetailsProvider()));

        objectStorageClient.setEndpoint(clientSettings.getEndpoint());

        return objectStorageClient;
    }

    /**
     * Refreshes the client settings of existing and new clients. Will not clear the cache of other
     * clients. Subsequent calls to {@code OciObjectStorageService#client} will return new clients
     * constructed using the parameter settings.
     *
     * @param clientsSettings the new settings used for building clients for subsequent requests
     */
    public synchronized void refreshWithoutClearingCache(
            Map<Settings, OciObjectStorageClientSettings> clientsSettings) {

        // build the new lazy clients
        final Map<Settings, LazyInitializable<ObjectStorageAsync, IOException>> oldClientCache =
                clientsCache.get();
        final MapBuilder<Settings, LazyInitializable<ObjectStorageAsync, IOException>>
                newClientsCache = MapBuilder.newMapBuilder();

        // replace or add new clients
        newClientsCache.putAll(oldClientCache);
        for (final Map.Entry<Settings, OciObjectStorageClientSettings> entry :
                clientsSettings.entrySet()) {
            final LazyInitializable<ObjectStorageAsync, IOException> previousClient =
                    oldClientCache.get(entry.getKey());
            newClientsCache.put(
                    entry.getKey(),
                    new LazyInitializable<>(
                            () -> createClientAsync(entry.getKey(), entry.getValue())));
            // we will release the previous client for this entry if existed
            if (previousClient != null) {
                previousClient.reset();
            }
        }
        clientsCache.getAndSet(newClientsCache.immutableMap());
    }

    /**
     * @param cacheKey
     */
    public synchronized void evictCache(Settings cacheKey) {

        final Map<Settings, LazyInitializable<ObjectStorageAsync, IOException>> oldClientCache =
                clientsCache.get();
        final MapBuilder<Settings, LazyInitializable<ObjectStorageAsync, IOException>>
                newClientsCache = MapBuilder.newMapBuilder();

        for (Map.Entry<Settings, LazyInitializable<ObjectStorageAsync, IOException>> entry :
                oldClientCache.entrySet()) {
            if (!entry.getKey().equals(cacheKey)) {
                newClientsCache.put(entry.getKey(), entry.getValue());
            }
        }
        clientsCache.getAndSet(newClientsCache.immutableMap());
    }

    @Override
    public void close() throws IOException {
        log.info("Shutting down all clients");
        clientsCache.get().values().stream()
                .forEach(
                        lazyClient -> {
                            try {
                                lazyClient.getOrCompute().close();
                            } catch (Exception e) {
                                log.error("unable to close client");
                            }
                        });
    }
}
