package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

public class DepositCommand implements Command {
    public final String exchange;
    public final String asset;
    private final String raw;

    public DepositCommand(String raw, String exchange, String asset) {
        this.raw = raw;
        this.exchange = exchange;
        this.asset = asset;
    }

    @Override
    public CommandType type() {
        return CommandType.DEPOSIT;
    }

    @Override
    public String raw() {
        return raw;
    }
}
