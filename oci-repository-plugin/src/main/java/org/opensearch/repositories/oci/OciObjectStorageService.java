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

import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.http.client.jersey.ApacheClientProperties;
import com.oracle.bmc.objectstorage.ObjectStorageAsync;
import com.oracle.bmc.objectstorage.ObjectStorageAsyncClient;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.logging.log4j.message.ParameterizedMessage;

/** Service class to hold client instances */
@Log4j2
public class OciObjectStorageService implements Closeable {
    /**
     * Dictionary of client instances. Client instances are built lazily from the latest settings.
     */
    private final Map<String, ObjectStorageAsync> clientsCache = new HashMap<>();

    /**
     * Attempts to retrieve a client from the cache. If the client does not exist it will be created
     * from the latest settings and will populate the cache. The returned instance should not be
     * cached by the calling code. Instead, for each use, the (possibly updated) instance should be
     * requested by calling this method.
     *
     * @param clientName name of the client settings used to create the client
     * @return a cached client storage instance that can be used to manage objects (blobs)
     */
    public synchronized ObjectStorageAsync client(
            final String clientName, OciObjectStorageClientSettings clientSettings)
            throws IOException {

        ObjectStorageAsync client = clientsCache.get(clientName);
        if (client == null) {
            client = createClientAsync(clientName, clientSettings);
            clientsCache.put(clientName, client);
        }
        return client;
    }

    /**
     * Creates a client that can be used to manage OCI Object Storage objects. The client is
     * thread-safe.
     *
     * @param clientName name of client settings to use, including secure settings
     * @param clientSettings name of client settings to use, including secure settings
     * @return a new client storage instance that can be used to manage objects (blobs)
     */
    static ObjectStorageAsync createClientAsync(
            String clientName, OciObjectStorageClientSettings clientSettings) throws IOException {
        log.debug(
                () ->
                        new ParameterizedMessage(
                                "creating OCI object store client with client_name [{}], endpoint"
                                        + " [{}]",
                                clientName,
                                clientSettings.getEndpoint()));

        final ObjectStorageAsync objectStorageClient =
                SocketAccess.doPrivilegedIOException(
                        () ->
                                ObjectStorageAsyncClient.builder()
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
                                                    poolConnectionManager.setMaxTotal(100);
                                                    poolConnectionManager.setDefaultMaxPerRoute(
                                                            100);

                                                    builder.property(
                                                            ApacheClientProperties
                                                                    .CONNECTION_MANAGER,
                                                            poolConnectionManager);
                                                })
                                        .configuration(ClientConfiguration.builder().build())
                                        .build(clientSettings.getAuthenticationDetailsProvider()));

        objectStorageClient.setEndpoint(clientSettings.getEndpoint());

        return objectStorageClient;
    }

    @Override
    public void close() throws IOException {
        log.info("Shutting down all clients");
        clientsCache.values().stream()
                .forEach(
                        lazyClient -> {
                            try {
                                lazyClient.close();
                            } catch (Exception e) {
                                log.error("unable to close client");
                            }
                        });
    }
}
