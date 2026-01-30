package com.crypto.console.repl;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.impl.CommandParser;
import com.crypto.console.common.command.CommandType;
import com.crypto.console.common.model.CommandResult;
import com.crypto.console.common.service.CommandExecutor;
import com.crypto.console.common.util.ConsoleOutput;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Slf4j
public class ReplRunner {

    private final CommandParser parser;
    private final CommandExecutor executor;

    public ReplRunner(CommandParser parser, CommandExecutor executor) {
        this.parser = parser;
        this.executor = executor;
    }

    public void run() {
        ConsoleOutput.printlnGreen("Crypto Console REPL. Type 'help' for commands, 'exit' to quit.");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                ConsoleOutput.printGreen("> ");
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                Command command = parser.parse(line);
                if (command.type() == CommandType.EXIT) {
                    ConsoleOutput.printlnGreen("Bye.");
                    break;
                }
                CommandResult result = executor.execute(command);
                if (result != null && result.message != null && !result.message.isEmpty()) {
                    ConsoleOutput.printlnGreen(result.message);
                }
            }
        } catch (Exception e) {
            LOG.error("REPL failed: {}", e.getMessage());
            System.err.println("REPL failed: " + e.getMessage());
        }
    }
}

