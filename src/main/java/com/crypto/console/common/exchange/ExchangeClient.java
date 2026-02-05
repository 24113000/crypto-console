package com.crypto.console.common.exchange;

import com.crypto.console.common.model.*;

import java.math.BigDecimal;

public interface ExchangeClient {
    String name();

    Balance getBalance(String asset);

    WithdrawalFees getWithdrawalFees(String asset);

    OrderBook getOrderBook(String base, String quote, int depth);

    BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount);

    BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount);

    OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount);

    OrderResult marketSell(String base, String quote, BigDecimal baseAmount);

    WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull);

    ExchangeTime syncTime();

    ExchangeCapabilities capabilities();
}



