package com.crypto.console.common.service;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.model.ExchangeException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DepositNetworkResolver {
    private final AppProperties config;

    public DepositNetworkResolver(AppProperties config) {
        this.config = config;
    }

    public Set<String> resolveDepositNetworks(ExchangeClient client, String exchange, String asset) {
        if (client.capabilities().supportsDepositNetworks && client instanceof DepositNetworkProvider provider) {
            Set<String> fromApi = provider.getDepositNetworks(asset);
            if (fromApi != null && !fromApi.isEmpty()) {
                return fromApi;
            }
        }

        Map<String, List<String>> exchangeFallback = config.getSupportedNetworksFallback() == null
                ? null
                : config.getSupportedNetworksFallback().get(exchange);
        if (exchangeFallback == null) {
            throw new ExchangeException("No supported network data for exchange: " + exchange);
        }
        List<String> assetNetworks = exchangeFallback.get(asset);
        if (assetNetworks == null || assetNetworks.isEmpty()) {
            throw new ExchangeException("No supported network data for " + exchange + " " + asset);
        }
        return new HashSet<>(assetNetworks);
    }
}


