package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

public class InvalidCommand implements Command {
    public final String error;
    private final String raw;

    public InvalidCommand(String raw, String error) {
        this.raw = raw;
        this.error = error;
    }

    @Override
    public CommandType type() {
        return CommandType.INVALID;
    }

    @Override
    public String raw() {
        return raw;
    }
}


