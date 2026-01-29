package com.crypto.console.exchanges.kucoin;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.exchange.BaseExchangeClient;
import com.crypto.console.common.model.*;

import java.math.BigDecimal;

public class KuCoinClient extends BaseExchangeClient {
    public KuCoinClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("kucoin", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        throw notImplemented("TODO: verify KuCoin balance endpoint");
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("TODO: verify KuCoin withdrawal fee endpoint");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("TODO: verify KuCoin order book endpoint");
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        throw notImplemented("TODO: verify KuCoin market buy endpoint");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        throw notImplemented("TODO: verify KuCoin market sell endpoint");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        throw notImplemented("TODO: verify KuCoin withdrawal endpoint");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("TODO: verify KuCoin server time endpoint");
    }
}





