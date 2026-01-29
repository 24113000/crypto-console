package com.crypto.console.exchanges.bitmart;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;

import java.math.BigDecimal;

public class BitMartClient extends BaseExchangeClient {
    public BitMartClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("bitmart", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        throw notImplemented("TODO: verify BitMart balance endpoint");
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("TODO: verify BitMart withdrawal fee endpoint");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("TODO: verify BitMart order book endpoint");
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        throw notImplemented("TODO: verify BitMart market buy endpoint");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        throw notImplemented("TODO: verify BitMart market sell endpoint");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        throw notImplemented("TODO: verify BitMart withdrawal endpoint");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("TODO: verify BitMart server time endpoint");
    }
}





