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

import lombok.extern.log4j.Log4j2;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.*;
import java.util.Base64;

/**
 * Crypto utilities used throughout the project
 */
@Log4j2
public class CryptoUtils {
    public static Path generatePrivatePublicKeyPair() throws NoSuchAlgorithmException, IOException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair kp = kpg.generateKeyPair();

        final Path keyFile = Files.createTempFile("tempPrivatePublicKeyPair", ".pem");
        log.info("creating a temp key file at: {}", keyFile);
        try (final BufferedWriter bufferedWriter =
                Files.newBufferedWriter(keyFile, StandardOpenOption.CREATE)) {
            bufferedWriter.write("-----BEGIN PRIVATE KEY-----\n");
            bufferedWriter.write(
                    Base64.getMimeEncoder().encodeToString(kp.getPrivate().getEncoded()) + "\n");
            bufferedWriter.write("-----END PRIVATE KEY-----\n");
            bufferedWriter.write("-----BEGIN PUBLIC KEY-----\n");
            bufferedWriter.write(
                    Base64.getMimeEncoder().encodeToString(kp.getPublic().getEncoded()) + "\n");
            bufferedWriter.write("-----END PUBLIC KEY-----\n");
            bufferedWriter.flush();
        }

        return keyFile;
    }
}
