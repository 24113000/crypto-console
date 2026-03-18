package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

public class BalancesCommand implements Command {
    public final String asset;
    private final String raw;

    public BalancesCommand(String raw, String asset) {
        this.raw = raw;
        this.asset = asset;
    }

    @Override
    public CommandType type() {
        return CommandType.BALANCES;
    }

    @Override
    public String raw() {
        return raw;
    }
}
