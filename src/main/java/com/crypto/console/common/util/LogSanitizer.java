package com.crypto.console.common.util;

import java.util.regex.Pattern;

public final class LogSanitizer {
    private static final Pattern SECRET_PATTERN = Pattern.compile("(?i)(apiKey|apiSecret|passphrase|memo|signature|sign|secret)=[^\s]+" );

    private LogSanitizer() {
    }

    public static String sanitize(String value) {
        if (value == null) {
            return null;
        }
        return SECRET_PATTERN.matcher(value).replaceAll("$1=***");
    }
}



