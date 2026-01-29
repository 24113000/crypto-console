package com.crypto.console.common.command;

import java.math.BigDecimal;

public class MoveCommand implements Command {
    public final String from;
    public final String to;
    public final BigDecimal amount;
    public final String asset;
    private final String raw;

    public MoveCommand(String raw, String from, String to, BigDecimal amount, String asset) {
        this.raw = raw;
        this.from = from;
        this.to = to;
        this.amount = amount;
        this.asset = asset;
    }

    @Override
    public CommandType type() {
        return CommandType.MOVE;
    }

    @Override
    public String raw() {
        return raw;
    }
}


