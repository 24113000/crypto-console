package com.crypto.console.common.service;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.exchange.ExchangeName;
import com.crypto.console.common.exchange.impl.ExchangeRegistry;
import com.crypto.console.common.model.Balance;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.model.WithdrawResult;
import com.crypto.console.common.model.WithdrawalFees;
import com.crypto.console.common.properties.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MoveService {
    private final ExchangeRegistry registry;
    private final AppProperties config;
    private final FeeResolver feeResolver;
    private final DepositNetworkResolver networkResolver;
    private final NetworkSelector networkSelector;

    public MoveService(ExchangeRegistry registry, AppProperties config, FeeResolver feeResolver, DepositNetworkResolver networkResolver, NetworkSelector networkSelector) {
        this.registry = registry;
        this.config = config;
        this.feeResolver = feeResolver;
        this.networkResolver = networkResolver;
        this.networkSelector = networkSelector;
    }

    public String move(String from, String to, BigDecimal amount, String asset) {
        ExchangeClient sender = registry.getClient(from);
        ExchangeClient recipient = registry.getClient(to);

        Balance baseline = recipient.getBalance(asset);
        BigDecimal baselineFree = baseline == null || baseline.free == null ? BigDecimal.ZERO : baseline.free;

        Set<String> networks = networkResolver.resolveDepositNetworks(recipient, to, asset);
        WithdrawalFees senderFees;
        try {
            senderFees = feeResolver.resolveWithdrawalFees(sender, from, asset);
        } catch (ExchangeException e) {
            // If fees are unavailable, proceed with zero fees for all networks.
            Map<String, BigDecimal> zeroFees = new java.util.HashMap<>();
            for (String network : networks) {
                zeroFees.put(network, BigDecimal.ZERO);
            }
            senderFees = new WithdrawalFees(asset, zeroFees);
        }

        String selectedNetwork = selectNetwork(to, asset, networks, senderFees);
        String recipientNetwork = selectedNetwork;
        if (recipient instanceof DepositNetworkNormalizer normalizer) {
            recipientNetwork = normalizer.normalizeDepositNetwork(selectedNetwork);
        }
        String senderNetwork = selectedNetwork;
        if (sender instanceof DepositNetworkNormalizer normalizer) {
            senderNetwork = normalizer.normalizeDepositNetwork(selectedNetwork);
        }
        AppProperties.AddressConfig addressConfig = getAddressConfig(to, asset, selectedNetwork);
        String address = addressConfig == null ? null : addressConfig.getAddress();
        String memo = addressConfig == null ? null : addressConfig.getMemo();
        if (StringUtils.isBlank(address) && recipient instanceof DepositAddressProvider provider) {
            address = provider.getDepositAddress(asset, recipientNetwork);
        }
        if (StringUtils.isBlank(address)) {
            if (isStubExchange(to)) {
                address = "STUB-ADDRESS";
            } else {
                throw new ExchangeException("Missing withdrawal address for " + to + " " + asset + " " + selectedNetwork);
            }
        }
        if (Boolean.TRUE.equals(addressConfig == null ? null : addressConfig.getMemoRequired()) && StringUtils.isBlank(memo)) {
            throw new ExchangeException("Memo/tag required for " + asset + " on " + selectedNetwork + " but missing in config");
        }

        LOG.info("Withdrawing {} {} via {} (senderNetwork={}) to {} (memo={})", amount, asset, selectedNetwork, senderNetwork, address, memo);
        WithdrawResult result = sender.withdraw(asset, amount, senderNetwork, address, memo);
        String withdrawalId = result == null ? "" : result.withdrawalId;

        boolean success = pollForDeposit(
                recipient,
                asset,
                baselineFree,
                config.getPolling().getIntervalSeconds(),
                config.getPolling().getMaxWaitSeconds()
        );
        if (!success) {
            throw new ExchangeException("Deposit not detected within timeout on " + to + " for " + asset);
        }

        return "Move submitted. WithdrawalId=" + withdrawalId + " network=" + selectedNetwork + " to=" + to;
    }

    private boolean pollForDeposit(ExchangeClient recipient, String asset, BigDecimal baseline, int intervalSeconds, int maxWaitSeconds) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(maxWaitSeconds));
        while (Instant.now().isBefore(deadline)) {
            Balance balance = recipient.getBalance(asset);
            if (balance != null && balance.free != null && balance.free.compareTo(baseline) > 0) {
                return true;
            }
            try {
                Thread.sleep(intervalSeconds * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ExchangeException("Polling interrupted", e);
            }
        }
        return false;
    }

    private String selectNetwork(String exchange, String asset, Set<String> networks, WithdrawalFees fees) {
        List<String> candidates = new ArrayList<>(networks);
        if (candidates.isEmpty()) {
            throw new ExchangeException("No deposit networks available for " + asset);
        }
        Map<String, BigDecimal> feeMap = fees.feeByNetwork;
        for (String network : candidates) {
            if (!feeMap.containsKey(network)) {
                throw new ExchangeException("Missing withdrawal fee for network " + network + " on asset " + asset);
            }
        }

        List<String> priority = config.getNetworkPriority() == null ? null : config.getNetworkPriority().get(asset);

        candidates.sort(Comparator
                .comparing((String n) -> feeMap.get(n))
                .thenComparing(n -> priorityIndex(priority, n))
                .thenComparing(String::compareTo));

        return networkSelector.selectNetwork(exchange, asset, candidates);
    }

    private int priorityIndex(List<String> priority, String network) {
        if (priority == null) {
            return Integer.MAX_VALUE;
        }
        int idx = priority.indexOf(network);
        return idx < 0 ? Integer.MAX_VALUE : idx;
    }

    private AppProperties.AddressConfig getAddressConfig(String exchange, String asset, String network) {
        if (config.getWithdrawalAddresses() == null) {
            return null;
        }
        Map<String, Map<String, AppProperties.AddressConfig>> exchangeMap = config.getWithdrawalAddresses().get(exchange);
        if (exchangeMap == null) {
            return null;
        }
        Map<String, AppProperties.AddressConfig> assetMap = exchangeMap.get(asset);
        if (assetMap == null) {
            return null;
        }
        return assetMap.get(network);
    }

    private boolean isStubExchange(String exchange) {
        ExchangeName name = ExchangeName.from(exchange);
        return name == ExchangeName.EXSTUB1 || name == ExchangeName.EXSTUB2;
    }

}
