package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.CommandType;

public class HelpCommand implements Command {
    private final String raw;

    public HelpCommand(String raw) {
        this.raw = raw;
    }

    @Override
    public CommandType type() {
        return CommandType.HELP;
    }

    @Override
    public String raw() {
        return raw;
    }
}


