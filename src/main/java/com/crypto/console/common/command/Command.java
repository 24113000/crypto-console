package com.crypto.console.common.command;

public interface Command {
    CommandType type();
    String raw();
}


