package com.crypto.console.common.command;

import java.math.BigDecimal;

public class BuyCommand implements Command {
    public final String exchange;
    public final String baseAsset;
    public final BigDecimal quoteAmount;
    public final String quoteAsset;
    private final String raw;

    public BuyCommand(String raw, String exchange, String baseAsset, BigDecimal quoteAmount, String quoteAsset) {
        this.raw = raw;
        this.exchange = exchange;
        this.baseAsset = baseAsset;
        this.quoteAmount = quoteAmount;
        this.quoteAsset = quoteAsset;
    }

    @Override
    public CommandType type() {
        return CommandType.BUY;
    }

    @Override
    public String raw() {
        return raw;
    }
}


