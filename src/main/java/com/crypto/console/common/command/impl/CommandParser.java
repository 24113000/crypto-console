package com.crypto.console.common.command.impl;

import com.crypto.console.common.command.Command;

import java.math.BigDecimal;

public class CommandParser {
    public Command parse(String line) {
        if (line == null) {
            return new InvalidCommand("", "Empty command");
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return new InvalidCommand(line, "Empty command");
        }
        String[] parts = trimmed.split("\\s+");
        String cmd = parts[0].toLowerCase();

        return switch (cmd) {
            case "move" -> parseMove(trimmed, parts);
            case "buy" -> parseBuy(trimmed, parts);
            case "sell" -> parseSell(trimmed, parts);
            case "balance" -> parseBalance(trimmed, parts);
            case "fees" -> parseFees(trimmed, parts);
            case "orderbook" -> parseOrderBook(trimmed, parts);
            case "help", "?" -> new HelpCommand(trimmed);
            case "exit", "quit" -> new ExitCommand(trimmed);
            default -> new InvalidCommand(trimmed, "Unknown command: " + parts[0]);
        };
    }

    private Command parseMove(String raw, String[] parts) {
        if (parts.length != 5) {
            return new InvalidCommand(raw, "Syntax: move <from> <to> <amount> <asset>");
        }
        BigDecimal amount = parsePositiveDecimal(parts[3], raw);
        if (amount == null) {
            return new InvalidCommand(raw, "Amount must be a positive number");
        }
        return new MoveCommand(raw, parts[1].toLowerCase(), parts[2].toLowerCase(), amount, parts[4].toUpperCase());
    }

    private Command parseBuy(String raw, String[] parts) {
        if (parts.length != 5) {
            return new InvalidCommand(raw, "Syntax: buy <exchange> <baseAsset> <quoteAmount> <quoteAsset>");
        }
        BigDecimal amount = parsePositiveDecimal(parts[3], raw);
        if (amount == null) {
            return new InvalidCommand(raw, "Quote amount must be a positive number");
        }
        return new BuyCommand(raw, parts[1].toLowerCase(), parts[2].toUpperCase(), amount, parts[4].toUpperCase());
    }

    private Command parseSell(String raw, String[] parts) {
        if (parts.length != 5) {
            return new InvalidCommand(raw, "Syntax: sell <exchange> <baseAsset> <baseAmount> <quoteAsset>");
        }
        BigDecimal amount = parsePositiveDecimal(parts[3], raw);
        if (amount == null) {
            return new InvalidCommand(raw, "Base amount must be a positive number");
        }
        return new SellCommand(raw, parts[1].toLowerCase(), parts[2].toUpperCase(), amount, parts[4].toUpperCase());
    }

    private Command parseBalance(String raw, String[] parts) {
        if (parts.length != 3) {
            return new InvalidCommand(raw, "Syntax: balance <exchange> <asset>");
        }
        return new BalanceCommand(raw, parts[1].toLowerCase(), parts[2].toUpperCase());
    }

    private Command parseFees(String raw, String[] parts) {
        if (parts.length != 3) {
            return new InvalidCommand(raw, "Syntax: fees <exchange> <asset>");
        }
        return new FeesCommand(raw, parts[1].toLowerCase(), parts[2].toUpperCase());
    }

    private Command parseOrderBook(String raw, String[] parts) {
        if (parts.length != 4) {
            return new InvalidCommand(raw, "Syntax: orderbook <exchange> <base> <quote>");
        }
        return new OrderBookCommand(raw, parts[1].toLowerCase(), parts[2].toUpperCase(), parts[3].toUpperCase());
    }

    private BigDecimal parsePositiveDecimal(String value, String raw) {
        try {
            BigDecimal amount = new BigDecimal(value);
            if (amount.signum() <= 0) {
                return null;
            }
            return amount;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}


