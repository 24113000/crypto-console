package com.crypto.console.common.command;

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


