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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.util.encoders.Hex;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.opensearch.common.bytes.BytesReference;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.x500.X500PrivateCredential;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.*;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;

@Log4j2
public class CryptoUtils {
    private static final Logger LOG = LogManager.getLogger(CryptoUtils.class);
    private static final byte[] FIXED_SALT = Hex.decode("0102030405060708090a0b0c0d0e0f10");
    private static final int FIXED_ITERATION_COUNT = 1024;
    private static final int FIXED_KEY_LENGTH = 256;

    public static final String ROOT_ALIAS = "root";
    private static final String DEFAULT_KEYSTORE_PASSWORD = "this-is-not-the-secret";

    public static char[] getDefaultKeystorePassword() {
        return DEFAULT_KEYSTORE_PASSWORD.toCharArray();
    }

    public static SecretKey makePbeKey(char[] password) {
        final SecretKey key;
        try {
            SecretKeyFactory keyFact =
                    SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512", "BCFIPS");
            key =
                    keyFact.generateSecret(
                            new PBEKeySpec(
                                    password, FIXED_SALT, FIXED_ITERATION_COUNT, FIXED_KEY_LENGTH));
        } catch (GeneralSecurityException e) {
            LOG.error("failed to calculate hash for password: {}", password, e);
            throw new RuntimeException(e);
        }

        return new SecretKeySpec(key.getEncoded(), "AES");
    }

    public static SecretKey makePbeKey(byte[] bytesPassword) {
        final char[] textPassword = toChar(bytesPassword);
        return makePbeKey(textPassword);
    }

    public static boolean checkPassword(BytesReference adminPasswordHashValue, char[] clearText) {
        final String encodedClearText = clearTextToEncodedStr(clearText);
        return encodedClearText.equals(Hex.toHexString(adminPasswordHashValue.toBytesRef().bytes));
    }

    private static char[] toChar(byte[] bytes) {
        final char[] chars = new char[bytes.length];

        for (int i = 0; i < bytes.length; i++) {
            /*
             * chars are treated as unicode in java and therefore the conversion mask
             * See https://stackoverflow.com/questions/17912640/byte-and-char-conversion-in-java/17912706
             *
             */
            chars[i] = (char) (bytes[i] & 0xFF);
        }

        return chars;
    }

    public static byte[] toBytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = Charset.forName("UTF-8").encode(charBuffer);
        byte[] bytes =
                Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
        return bytes;
    }

    public static String clearTextToEncodedStr(char[] clearText) {
        final byte[] encodedBytes = makePbeKey(clearText).getEncoded();
        return Hex.toHexString(encodedBytes);
    }

    public static KeyStore createKeyStoreFromPemFiles(
            byte[] rootPublicKeyFile, byte[] rootPrivateKeyFile, byte[] intermediateCerts)
            throws Exception {
        X500PrivateCredential serverCred =
                createRootCredential(rootPublicKeyFile, rootPrivateKeyFile);
        KeyStore keyStore = KeyStore.getInstance("BCFKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry(
                serverCred.getAlias(),
                serverCred.getPrivateKey(),
                getDefaultKeystorePassword(),
                new X509Certificate[] {
                    serverCred.getCertificate(), generateRootCert(intermediateCerts)
                });
        return keyStore;
    }

    public static byte[] loadPEM(File pemFile) throws IOException {
        try (final PemReader pemReader = new PemReader(Files.newBufferedReader(pemFile.toPath()))) {
            final PemObject pemObject = pemReader.readPemObject();

            return pemObject.getContent();
        }
    }

    public static X500PrivateCredential createRootCredential(
            byte[] rootPublicKeyFile, byte[] rootPrivateKeyFile) throws Exception {
        X509Certificate rootCert = generateRootCert(rootPublicKeyFile);
        KeyPair rootPair = generateRootKeyPair(rootCert, rootPrivateKeyFile);

        return new X500PrivateCredential(rootCert, rootPair.getPrivate(), ROOT_ALIAS);
    }

    /** Create a fixed 2048 bit RSA key pair – to keep the server cert stable */
    public static KeyPair generateRootKeyPair(X509Certificate rootCert, byte[] rootPrivateKeyFile)
            throws Exception {
        final PublicKey publicKey = rootCert.getPublicKey();
        final PEMFileRSAPrivateKeySupplier supplier =
                PEMFileRSAPrivateKeySupplier.newInstance(rootPrivateKeyFile);
        final PrivateKey privateKey = supplier.getKey("stub").get();
        return new KeyPair(publicKey, privateKey);
    }

    public static X509Certificate generateRootCert(byte[] certificateFile) throws Exception {
        CertificateFactory cf =
                CertificateFactory.getInstance("X.509", BouncyCastleFipsProvider.PROVIDER_NAME);
        final X509Certificate certificate =
                (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(certificateFile));

        return certificate;
    }

    public static KeyStore createTrustStoreFromPemFiles(Path rootCaPath) throws Exception {
        CertificateFactory cf =
                CertificateFactory.getInstance("X.509", BouncyCastleFipsProvider.PROVIDER_NAME);
        final X509Certificate trustCert =
                (X509Certificate) cf.generateCertificate(Files.newInputStream(rootCaPath));

        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setCertificateEntry("ROOT-CA-MADE-UP-ALIAS", trustCert);
        return keyStore;
    }

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

    public static String extractCommonNameFromCert(X509Certificate certificate)
            throws CertificateEncodingException {
        final X500Name x500name = new JcaX509CertificateHolder(certificate).getSubject();
        final RDN cn = x500name.getRDNs(BCStyle.CN)[0];
        return IETFUtils.valueToString(cn.getFirst().getValue());
    }

    public static boolean isPkiCertCommonNameValid(
            String certificateCommonName, String expectedCommonNames) {
        final String[] expectedCommonNamesArray = expectedCommonNames.split(",");
        for (String expectedCommonName : expectedCommonNamesArray) {
            // simpleString settings will take off the '*.' from the start of the expected CN name.
            // modifying this method to handle such use case...
            final String currentCertificateCommonName =
                    certificateCommonName.startsWith("*")
                            ? certificateCommonName.substring(2)
                            : certificateCommonName;
            if (expectedCommonName.equals(currentCertificateCommonName)) {
                return true;
            }
        }
        return false;
    }
}
