package com.crypto.console.common.command;

public class OrderBookCommand implements Command {
    public final String exchange;
    public final String base;
    public final String quote;
    private final String raw;

    public OrderBookCommand(String raw, String exchange, String base, String quote) {
        this.raw = raw;
        this.exchange = exchange;
        this.base = base;
        this.quote = quote;
    }

    @Override
    public CommandType type() {
        return CommandType.ORDERBOOK;
    }

    @Override
    public String raw() {
        return raw;
    }
}


