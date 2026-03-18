package com.crypto.console.common.exchange;

import com.crypto.console.common.model.ExchangeException;

public enum ExchangeName {
    ASCENDEX("ascendex", "asc"),
    BINANCE("binance", "bin"),
    BINGX("bingx", "bgx"),
    BITGET("bitget", "btg"),
    BITMART("bitmart", "bmt"),
    BITRUE("bitrue", "btr"),
    BYBIT("bybit", "byb"),
    COINEX("coinex", "cnx"),
    GATEIO("gateio", "gio"),
    HITBTC("hitbtc", "hit"),
    HTX("htx", "htx"),
    KRAKEN("kraken", "krk"),
    KUCOIN("kucoin", "kuc"),
    LBANK("lbank", "lbk"),
    MEXC("mexc", "mxc"),
    OKX("okx", "okx"),
    POLONIEX("poloniex", "plx"),
    XT("xt", "xt"),

    //stub
    EXSTUB1("exstub1", "EX1"),
    EXSTUB2("exstub2", "EX2");

    private final String id;
    private final String alias;

    ExchangeName(String id) {
        this(id, null);
    }

    ExchangeName(String id, String alias) {
        this.id = id;
        this.alias = alias;
    }

    public String id() {
        return id;
    }

    public static ExchangeName from(String value) {
        if (value == null || value.isBlank()) {
            throw new ExchangeException("Exchange name is required");
        }
        String normalized = canonical(value);
        for (ExchangeName name : values()) {
            if (canonical(name.id).equals(normalized) || canonical(name.alias).equals(normalized)) {
                return name;
            }
        }
        if ("ascend".equals(normalized)) {
            return ASCENDEX;
        }
        if ("gate".equals(normalized)) {
            return GATEIO;
        }
        if ("bittrue".equals(normalized)) {
            return BITRUE;
        }
        throw new ExchangeException("Unsupported exchange: " + value);
    }

    private static String canonical(String raw) {
        if (raw == null) {
            return "";
        }
        String trimmed = raw.trim().replace("\uFEFF", "");
        return trimmed.toLowerCase().replaceAll("[^a-z0-9]", "");
    }
}
