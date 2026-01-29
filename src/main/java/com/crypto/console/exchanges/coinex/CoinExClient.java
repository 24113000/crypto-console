package com.crypto.console.exchanges.coinex;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.exchange.BaseExchangeClient;
import com.crypto.console.common.model.*;

import java.math.BigDecimal;

public class CoinExClient extends BaseExchangeClient {
    public CoinExClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("coinex", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        throw notImplemented("TODO: verify CoinEx balance endpoint");
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("TODO: verify CoinEx withdrawal fee endpoint");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("TODO: verify CoinEx order book endpoint");
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        throw notImplemented("TODO: verify CoinEx market buy endpoint");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        throw notImplemented("TODO: verify CoinEx market sell endpoint");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        throw notImplemented("TODO: verify CoinEx withdrawal endpoint");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("TODO: verify CoinEx server time endpoint");
    }
}





