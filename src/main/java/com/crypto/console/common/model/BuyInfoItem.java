package com.crypto.console.common.model;

import java.math.BigDecimal;

public class BuyInfoItem {
    public final BigDecimal price;
    public final BigDecimal filledBaseAmount;
    public final BigDecimal spentQuoteAmount;

    public BuyInfoItem(BigDecimal price, BigDecimal filledBaseAmount, BigDecimal spentQuoteAmount) {
        this.price = price;
        this.filledBaseAmount = filledBaseAmount;
        this.spentQuoteAmount = spentQuoteAmount;
    }
}
