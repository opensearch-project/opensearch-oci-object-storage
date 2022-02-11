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

package org.opensearch.common.crypto;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for anything that can get supply a Key of type T given a keyid
 *
 * @param <T> the type of the key that will be supplied
 */
public interface KeySupplier<T> {

    /*
     * Try to get a key for the given KeyId
     *
     * @param keyId
     *     the identifier of the key to try to supply
     *
     * @return a Callable that will try to get the key when executed.
     *
     * The callable returns an Optional of T. If the key cannot be found the Optional will be empty.
     */
    @Nonnull
    Optional<T> getKey(@Nonnull final String keyId);

    /**
     * A key supplier can generate claims. For example, security token generated for browser auth
     * contains a JWT token inside the token itself
     *
     * @param key the key
     * @return a map of key value pair. A single key can have more than one values.
     */
    default Map<String, String> getClaims(final String key) {
        return new HashMap<>();
    }

    /**
     * Gets the issuer of the claims.
     *
     * @return
     */
    default Optional<String> getIssuer(final String key) {
        return Optional.empty();
    }
}
