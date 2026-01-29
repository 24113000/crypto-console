package com.crypto.console.common.command;

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


