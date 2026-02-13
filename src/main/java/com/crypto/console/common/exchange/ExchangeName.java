package com.crypto.console.common.exchange;

import com.crypto.console.common.model.ExchangeException;

public enum ExchangeName {
    BINANCE("binance"),
    MEXC("mexc"),
    XT("xt"),
    COINEX("coinex"),
    BITMART("bitmart"),
    KUCOIN("kucoin"),
    GATEIO("gateio"),
    HTX("htx"),
    LBANK("lbank"),
    ASCENDEX("ascendex"),
    HITBTC("hitbtc"),
    BINGX("bingx"),
    BITGET("bitget"),
    BYBIT("bybit"),
    BITRUE("bitrue"),
    POLONIEX("poloniex"),

    //stub
    EXSTUB1("exstub1"),
    EXSTUB2("exstub2");

    private final String id;

    ExchangeName(String id) {
        this.id = id;
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
            if (canonical(name.id).equals(normalized)) {
                return name;
            }
        }
        if ("ascend".equals(normalized)) {
            return ASCENDEX;
        }
        if ("gateio".equals(normalized)) {
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
