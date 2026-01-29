package com.crypto.console.common.model;

public class ExchangeTime {
    public final long serverTimeMillis;
    public final long offsetMillis;

    public ExchangeTime(long serverTimeMillis, long offsetMillis) {
        this.serverTimeMillis = serverTimeMillis;
        this.offsetMillis = offsetMillis;
    }
}



