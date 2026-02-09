package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

import java.math.BigDecimal;

public class SpreadCommand implements Command {
    public final String exchange1;
    public final String exchange2;
    public final String baseAsset;
    public final BigDecimal quoteAmount;
    public final String quoteAsset;
    private final String raw;

    public SpreadCommand(String raw, String exchange1, String exchange2, String baseAsset, BigDecimal quoteAmount, String quoteAsset) {
        this.raw = raw;
        this.exchange1 = exchange1;
        this.exchange2 = exchange2;
        this.baseAsset = baseAsset;
        this.quoteAmount = quoteAmount;
        this.quoteAsset = quoteAsset;
    }

    @Override
    public CommandType type() {
        return CommandType.SPREAD;
    }

    @Override
    public String raw() {
        return raw;
    }
}
