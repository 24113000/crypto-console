package com.crypto.console.common.model;

import java.util.List;

public class OrderBook {
    public final String symbol;
    public final List<OrderBookEntry> bids;
    public final List<OrderBookEntry> asks;

    public OrderBook(String symbol, List<OrderBookEntry> bids, List<OrderBookEntry> asks) {
        this.symbol = symbol;
        this.bids = bids;
        this.asks = asks;
    }
}



