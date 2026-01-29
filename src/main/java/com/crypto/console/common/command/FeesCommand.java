package com.crypto.console.common.command;

public class FeesCommand implements Command {
    public final String exchange;
    public final String asset;
    private final String raw;

    public FeesCommand(String raw, String exchange, String asset) {
        this.raw = raw;
        this.exchange = exchange;
        this.asset = asset;
    }

    @Override
    public CommandType type() {
        return CommandType.FEES;
    }

    @Override
    public String raw() {
        return raw;
    }
}


