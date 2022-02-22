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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEInputDecryptorProviderBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.interfaces.RSAPrivateKey;
import java.util.Optional;

/**
 * An implementation of {@link KeySupplier} that supplies a RSA private key from a PEM file.
 * Supports both PKCS#8 (starts with '-----BEGIN PRIVATE KEY-----' tag) and PKCS#1 (i.e., starts
 * with '-----BEGIN RSA PRIVATE KEY-----' tag) format.
 *
 * <p>Example commands that can be used to generate a 2048 bits RSA private key: <code>
 * $ openssl genrsa -out privateKey 2048</code>
 *
 * <p><code>$ ssh-keygen -t rsa -b 2048</code>
 *
 * <p>
 */
@Log4j2
public final class PEMFileRSAPrivateKeySupplier implements KeySupplier<RSAPrivateKey> {
    private final JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

    private final RSAPrivateKey key;

    /**
     * Constructs a new file key supplier which reads the private key from the specified file.
     *
     * @param key the RSA private key
     * @return An instance of {@link PEMFileRSAPrivateKeySupplier}
     */
    public static PEMFileRSAPrivateKeySupplier newInstance(@Nonnull final byte key[]) {
        return new PEMFileRSAPrivateKeySupplier(key, null);
    }

    /**
     * Constructs a new file key supplier which reads an encrypted private key from the specified
     * file using the specified password to decrypt key.
     *
     * @param key the RSA private key file
     * @param password the passphrase of the private key
     * @return An instance of {@link PEMFileRSAPrivateKeySupplier}
     */
    public static PEMFileRSAPrivateKeySupplier newInstance(
            @Nonnull final byte key[], @Nonnull final String password) {
        Preconditions.checkArgument(!StringUtils.isBlank(password));

        return new PEMFileRSAPrivateKeySupplier(key, password);
    }

    /**
     * Constructs a new file key supplier which reads the private key from the specified file.
     *
     * @param key the RSA private key
     * @param password the passphrase of the private key, optional
     */
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.CloseResource"})
    private PEMFileRSAPrivateKeySupplier(
            @Nonnull final byte key[], @Nullable final String password) {
        Preconditions.checkArgument(
                key != null && key.length != 0, "private key filename cannot be empty");

        PEMParser keyReader = null;
        try {
            log.debug("Initializing private key"); // don't log password

            keyReader =
                    new PEMParser(
                            new InputStreamReader(new ByteArrayInputStream(key), Charsets.UTF_8));
            Object object = keyReader.readObject();

            final PrivateKeyInfo keyInfo;

            if (object instanceof PEMEncryptedKeyPair) {
                Preconditions.checkNotNull(
                        password, "Password is null, but a password is required");

                PEMDecryptorProvider decProv =
                        new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
                keyInfo =
                        ((PEMEncryptedKeyPair) object).decryptKeyPair(decProv).getPrivateKeyInfo();
            } else if (object instanceof PrivateKeyInfo) {
                keyInfo = (PrivateKeyInfo) object;
            } else if (object instanceof PKCS8EncryptedPrivateKeyInfo) {
                PKCS8EncryptedPrivateKeyInfo encPrivKeyInfo = (PKCS8EncryptedPrivateKeyInfo) object;
                InputDecryptorProvider pkcs8Prov =
                        new JcePKCSPBEInputDecryptorProviderBuilder().build(password.toCharArray());
                keyInfo = encPrivKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
            } else {
                keyInfo = ((PEMKeyPair) object).getPrivateKeyInfo();
            }

            this.key = (RSAPrivateKey) converter.getPrivateKey(keyInfo);
        } catch (IOException | PKCSException ex) {
            log.debug("Failed to read RSA private key", ex);
            throw Throwables.propagate(ex);
        } finally {
            if (keyReader != null) {
                IOUtils.closeQuietly(keyReader);
            }
        }
    }

    @Nonnull
    @Override
    public Optional<RSAPrivateKey> getKey(@Nonnull String keyId) {
        return Optional.of(key);
    }
}
