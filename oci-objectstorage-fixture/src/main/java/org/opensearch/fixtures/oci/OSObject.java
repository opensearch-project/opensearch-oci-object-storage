package org.opensearch.fixtures.oci;

import lombok.Getter;

import java.time.Instant;

public class OSObject {
    @Getter private final String prefix;
    private final byte[] bytes;
    @Getter private Instant lastModified = Instant.now();

    public OSObject(String prefix, byte[] bytes) {
        this.prefix = prefix;
        this.bytes = new byte[bytes.length];
        System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
    }

    public byte[] getBytes() {
        final byte[] copy = new byte[bytes.length];
        System.arraycopy(bytes, 0, copy, 0, bytes.length);
        return copy;
    }
}
