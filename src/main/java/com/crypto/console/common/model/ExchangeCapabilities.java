package com.crypto.console.common.model;

public class ExchangeCapabilities {
    public final boolean supportsBalances;
    public final boolean supportsWithdrawalFees;
    public final boolean supportsOrderBook;
    public final boolean supportsMarketOrders;
    public final boolean supportsWithdrawals;
    public final boolean supportsTimeSync;
    public final boolean supportsDepositNetworks;

    public ExchangeCapabilities(
            boolean supportsBalances,
            boolean supportsWithdrawalFees,
            boolean supportsOrderBook,
            boolean supportsMarketOrders,
            boolean supportsWithdrawals,
            boolean supportsTimeSync,
            boolean supportsDepositNetworks
    ) {
        this.supportsBalances = supportsBalances;
        this.supportsWithdrawalFees = supportsWithdrawalFees;
        this.supportsOrderBook = supportsOrderBook;
        this.supportsMarketOrders = supportsMarketOrders;
        this.supportsWithdrawals = supportsWithdrawals;
        this.supportsTimeSync = supportsTimeSync;
        this.supportsDepositNetworks = supportsDepositNetworks;
    }
}



