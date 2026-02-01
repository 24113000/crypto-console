package com.crypto.console.common.service;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.exchange.ExchangeName;
import com.crypto.console.common.exchange.impl.ExchangeRegistry;
import com.crypto.console.common.model.Balance;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.model.WithdrawResult;
import com.crypto.console.common.properties.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class MoveService {
    private final ExchangeRegistry registry;
    private final AppProperties config;
    private final DepositNetworkResolver networkResolver;
    private final NetworkSelector networkSelector;

    public MoveService(ExchangeRegistry registry, AppProperties config, DepositNetworkResolver networkResolver, NetworkSelector networkSelector) {
        this.registry = registry;
        this.config = config;
        this.networkResolver = networkResolver;
        this.networkSelector = networkSelector;
    }

    public String move(String from, String to, BigDecimal amount, String asset) {
        ExchangeClient sender = registry.getClient(from);
        ExchangeClient recipient = registry.getClient(to);

        Balance baseline = recipient.getBalance(asset);
        BigDecimal baselineFree = baseline == null || baseline.free == null ? BigDecimal.ZERO : baseline.free;

        Set<String> recipientNetworks = networkResolver.resolveDepositNetworks(recipient, to, asset);
        Set<String> senderNetworks = resolveSenderNetworks(sender, recipientNetworks, asset);

        String selectedRecipientNetwork = selectNetwork(to, asset, recipientNetworks);
        String selectedSenderNetwork = selectNetwork(from, asset, senderNetworks);

        String recipientNetwork = selectedRecipientNetwork;
        if (recipient instanceof DepositNetworkNormalizer normalizer) {
            recipientNetwork = normalizer.normalizeDepositNetwork(selectedRecipientNetwork);
        }
        String senderNetwork = selectedSenderNetwork;
        if (sender instanceof DepositNetworkNormalizer normalizer) {
            senderNetwork = normalizer.normalizeDepositNetwork(selectedSenderNetwork);
        }
        AppProperties.AddressConfig addressConfig = getAddressConfig(to, asset, selectedRecipientNetwork);
        String address = addressConfig == null ? null : addressConfig.getAddress();
        String memo = addressConfig == null ? null : addressConfig.getMemo();
        if (StringUtils.isBlank(address) && recipient instanceof DepositAddressProvider provider) {
            address = provider.getDepositAddress(asset, recipientNetwork);
        }
        if (StringUtils.isBlank(address)) {
            throw new ExchangeException("Missing withdrawal address for " + to + " " + asset + " " + selectedRecipientNetwork);
        }
        if (Boolean.TRUE.equals(addressConfig == null ? null : addressConfig.getMemoRequired()) && StringUtils.isBlank(memo)) {
            throw new ExchangeException("Memo/tag required for " + asset + " on " + selectedRecipientNetwork + " but missing in config");
        }

        if (!confirmTransfer(from, to, asset, amount, selectedSenderNetwork, selectedRecipientNetwork, senderNetwork, recipientNetwork, address, memo)) {
            throw new ExchangeException("Transfer cancelled by user");
        }

        LOG.info("Withdrawing {} {} via {} (senderNetwork={}) to {} (memo={})", amount, asset, selectedSenderNetwork, senderNetwork, address, memo);
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

        return "Move submitted. WithdrawalId=" + withdrawalId + " network=" + selectedSenderNetwork + " to=" + to;
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

    private String selectNetwork(String exchange, String asset, Set<String> networks) {
        List<String> candidates = new ArrayList<>(networks);
        if (candidates.isEmpty()) {
            throw new ExchangeException("No deposit networks available for " + asset);
        }
        List<String> priority = config.getNetworkPriority() == null ? null : config.getNetworkPriority().get(asset);

        candidates.sort(Comparator
                .comparing((String n) -> priorityIndex(priority, n))
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

    private Set<String> resolveSenderNetworks(ExchangeClient sender, Set<String> fallback, String asset) {
        if (sender.capabilities().supportsDepositNetworks && sender instanceof DepositNetworkProvider provider) {
            Set<String> networks = provider.getDepositNetworks(asset);
            if (networks != null && !networks.isEmpty()) {
                return networks;
            }
        }
        return fallback;
    }

    private boolean confirmTransfer(String from, String to, String asset, BigDecimal amount,
                                    String selectedSenderNetwork, String selectedRecipientNetwork,
                                    String senderNetwork, String recipientNetwork,
                                    String address, String memo) {
        String y = ThreadLocalRandom.current().nextBoolean() ? "Y" : "y";
        String n = "Y".equals(y) ? "n" : "N";
        System.out.println("Transfer details:");
        System.out.println("  from: " + from);
        System.out.println("  to: " + to);
        System.out.println("  asset: " + asset);
        System.out.println("  amount: " + amount);
        System.out.println("  sender network: " + selectedSenderNetwork + " (normalized=" + senderNetwork + ")");
        System.out.println("  recipient network: " + selectedRecipientNetwork + " (normalized=" + recipientNetwork + ")");
        System.out.println("  address: " + address);
        System.out.println("  memo: " + (memo == null ? "" : memo));
        System.out.print("Approve transfer? [" + y + "/" + n + "]: ");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line = reader.readLine();
            if (line == null) {
                return false;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                return false;
            }
            char c = trimmed.charAt(0);
            if (c == y.charAt(0)) {
                return true;
            }
            if (c == n.charAt(0)) {
                return false;
            }
            return false;
        } catch (Exception e) {
            throw new ExchangeException("Failed to read approval input", e);
        }
    }

}
