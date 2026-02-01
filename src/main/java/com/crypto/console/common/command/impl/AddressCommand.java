package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

public class AddressCommand implements Command {
    public final String exchange;
    public final String asset;
    public final String network;
    private final String raw;

    public AddressCommand(String raw, String exchange, String asset, String network) {
        this.raw = raw;
        this.exchange = exchange;
        this.asset = asset;
        this.network = network;
    }

    @Override
    public CommandType type() {
        return CommandType.ADDRESS;
    }

    @Override
    public String raw() {
        return raw;
    }
}
