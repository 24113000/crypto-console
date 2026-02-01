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
        String normalized = value.trim().toLowerCase();
        for (ExchangeName name : values()) {
            if (name.id.equals(normalized)) {
                return name;
            }
        }
        throw new ExchangeException("Unsupported exchange: " + value);
    }
}
