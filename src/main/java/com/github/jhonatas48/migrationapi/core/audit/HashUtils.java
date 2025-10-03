package com.github.jhonatas48.migrationapi.core.audit;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Formatter;

public final class HashUtils {
    private HashUtils() {}

    public static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(input.getBytes(StandardCharsets.UTF_8));
            try (Formatter f = new Formatter()) {
                for (byte b : dig) {
                    f.format("%02x", b);
                }
                return f.toString();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Erro ao calcular SHA-256", e);
        }
    }
}