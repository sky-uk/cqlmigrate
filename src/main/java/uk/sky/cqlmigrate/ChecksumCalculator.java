package uk.sky.cqlmigrate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class ChecksumCalculator {

    static String calculateChecksum(Path path) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-1");
            final byte[] hash = digest.digest(Files.readAllBytes(path));
            return bytesToHex(hash);
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        final StringBuilder builder = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            final int asUnsigned = Byte.toUnsignedInt(b);
            builder.append(Character.forDigit(asUnsigned >>> 4, 16))
                    .append(Character.forDigit(asUnsigned & 0x0F, 16));
        }
        return builder.toString();
    }
}
