package com.crypto.console.exchanges.mexc;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;

import java.math.BigDecimal;

public class MexcClient extends BaseExchangeClient {
    public MexcClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("mexc", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        throw notImplemented("GET /api/v3/account (signed)");
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /api/v3/capital/config/getall (signed)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /api/v3/depth (public)");
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        throw notImplemented("POST /api/v3/order (signed)");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        throw notImplemented("POST /api/v3/order (signed)");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        throw notImplemented("POST /api/v3/capital/withdraw/apply (signed)");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("GET /api/v3/time (public)");
    }
}





