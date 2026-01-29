package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

public class ExitCommand implements Command {
    private final String raw;

    public ExitCommand(String raw) {
        this.raw = raw;
    }

    @Override
    public CommandType type() {
        return CommandType.EXIT;
    }

    @Override
    public String raw() {
        return raw;
    }
}


