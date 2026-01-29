package com.crypto.console.common.model;

import java.math.BigDecimal;

public class Balance {
    public final String asset;
    public final BigDecimal free;
    public final BigDecimal locked;

    public Balance(String asset, BigDecimal free, BigDecimal locked) {
        this.asset = asset;
        this.free = free;
        this.locked = locked;
    }
}



