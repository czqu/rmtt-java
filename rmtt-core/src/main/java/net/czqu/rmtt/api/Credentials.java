package net.czqu.rmtt.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Helpers for keeping raw credentials out of logs. Credentials may carry tokens or
 * user-password pairs, so log statements must never echo the original value.
 */
public final class Credentials {

    private Credentials() {
    }

    /**
     * Render a credential for logging without revealing it: the length plus a short digest
     * prefix that still allows correlating repeated occurrences of the same credential.
     *
     * @param credential the raw credential (may be null)
     * @return a log-safe rendering such as {@code len=12#sha256=3f2a1c0b}
     */
    public static String mask(String credential) {
        if (credential == null) {
            return "null";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(credential.getBytes(StandardCharsets.UTF_8));
            return "len=" + credential.length() + "#sha256="
                    + String.format("%02x%02x%02x%02x", d[0], d[1], d[2], d[3]);
        } catch (Exception e) {
            return "len=" + credential.length();
        }
    }
}
