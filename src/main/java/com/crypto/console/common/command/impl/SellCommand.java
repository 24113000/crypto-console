package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

import java.math.BigDecimal;

public class SellCommand implements Command {
    public final String exchange;
    public final String baseAsset;
    public final BigDecimal baseAmount;
    public final String quoteAsset;
    private final String raw;

    public SellCommand(String raw, String exchange, String baseAsset, BigDecimal baseAmount, String quoteAsset) {
        this.raw = raw;
        this.exchange = exchange;
        this.baseAsset = baseAsset;
        this.baseAmount = baseAmount;
        this.quoteAsset = quoteAsset;
    }

    @Override
    public CommandType type() {
        return CommandType.SELL;
    }

    @Override
    public String raw() {
        return raw;
    }
}


