package com.crypto.console.common.model;

import java.math.BigDecimal;
import java.util.List;

public class BuyInfoResult {
    public final String symbol;
    public final BigDecimal requestedQuoteAmount;
    public final BigDecimal spentQuoteAmount;
    public final BigDecimal boughtBaseAmount;
    public final BigDecimal averagePrice;
    public final List<BuyInfoItem> affectedOrderBookItems;

    public BuyInfoResult(
            String symbol,
            BigDecimal requestedQuoteAmount,
            BigDecimal spentQuoteAmount,
            BigDecimal boughtBaseAmount,
            BigDecimal averagePrice,
            List<BuyInfoItem> affectedOrderBookItems
    ) {
        this.symbol = symbol;
        this.requestedQuoteAmount = requestedQuoteAmount;
        this.spentQuoteAmount = spentQuoteAmount;
        this.boughtBaseAmount = boughtBaseAmount;
        this.averagePrice = averagePrice;
        this.affectedOrderBookItems = affectedOrderBookItems;
    }
}
