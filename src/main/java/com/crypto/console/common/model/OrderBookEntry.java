package com.crypto.console.common.model;

import java.math.BigDecimal;

public class OrderBookEntry {
    public final BigDecimal price;
    public final BigDecimal quantity;

    public OrderBookEntry(BigDecimal price, BigDecimal quantity) {
        this.price = price;
        this.quantity = quantity;
    }
}



