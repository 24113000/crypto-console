package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

import java.math.BigDecimal;

public class SellInfoCommand implements Command {
    public final String exchange;
    public final String baseAsset;
    public final BigDecimal quoteAmount;
    public final String quoteAsset;
    private final String raw;

    public SellInfoCommand(String raw, String exchange, String baseAsset, BigDecimal quoteAmount, String quoteAsset) {
        this.raw = raw;
        this.exchange = exchange;
        this.baseAsset = baseAsset;
        this.quoteAmount = quoteAmount;
        this.quoteAsset = quoteAsset;
    }

    @Override
    public CommandType type() {
        return CommandType.SELLINFO;
    }

    @Override
    public String raw() {
        return raw;
    }
}
