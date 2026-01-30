package com.crypto.console.exchanges.exstub1;

import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.Balance;
import com.crypto.console.common.model.ExchangeCapabilities;
import com.crypto.console.common.model.ExchangeTime;
import com.crypto.console.common.model.OrderBook;
import com.crypto.console.common.model.OrderResult;
import com.crypto.console.common.model.WithdrawResult;
import com.crypto.console.common.model.WithdrawalFees;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ExStub1Client extends BaseExchangeClient implements DepositNetworkProvider {
    public ExStub1Client(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("exstub1", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        LOG.info("exstub1 getBalance asset={}", asset);
        return new Balance(asset, BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        LOG.info("exstub1 getWithdrawalFees asset={}", asset);
        return new WithdrawalFees(asset, Map.of(
                "STUBNET", BigDecimal.ONE,
                "STUBNET2", BigDecimal.TEN
        ));
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        LOG.info("exstub1 getOrderBook base={} quote={} depth={}", base, quote, depth);
        return new OrderBook(base + quote, List.of(), List.of());
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        LOG.info("exstub1 marketBuy base={} quote={} quoteAmount={}", base, quote, quoteAmount);
        return new OrderResult("stub-buy-1", "STUB", "stub order");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        LOG.info("exstub1 marketSell base={} quote={} baseAmount={}", base, quote, baseAmount);
        return new OrderResult("stub-sell-1", "STUB", "stub order");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        LOG.info("exstub1 withdraw asset={} amount={} network={} address={} memo={}", asset, amount, network, address, memoOrNull);
        return new WithdrawResult("stub-withdraw-1", "STUB", "stub withdraw");
    }

    @Override
    public ExchangeTime syncTime() {
        LOG.info("exstub1 syncTime");
        return new ExchangeTime(System.currentTimeMillis(), 0);
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        LOG.info("exstub1 getDepositNetworks asset={}", asset);
        return Set.of("STUBNET");
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, true, true, true, true, false, true);
    }
}
