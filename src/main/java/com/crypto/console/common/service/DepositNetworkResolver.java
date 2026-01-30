package com.crypto.console.common.service;

import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.properties.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public class DepositNetworkResolver {
    public DepositNetworkResolver(AppProperties config) {
    }

    public Set<String> resolveDepositNetworks(ExchangeClient destination, String exchange, String asset) {
        Set<String> fromApi = getFromDestination(destination, asset);
        if (!fromApi.isEmpty()) {
            LOG.info("Using deposit networks from destination exchange {} for asset {}", exchange, asset);
            return fromApi;
        }

        throw new ExchangeException("No deposit networks returned by destination exchange: " + exchange + " " + asset);
    }

    private Set<String> getFromDestination(ExchangeClient destination, String asset) {
        if (!destination.capabilities().supportsDepositNetworks || !(destination instanceof DepositNetworkProvider provider)) {
            return Set.of();
        }
        Set<String> networks = provider.getDepositNetworks(asset);
        if (networks == null || networks.isEmpty()) {
            return Set.of();
        }
        return normalizeNetworks(networks);
    }

    private Set<String> normalizeNetworks(Iterable<String> networks) {
        Set<String> normalized = new HashSet<>();
        for (String network : networks) {
            if (StringUtils.isBlank(network)) {
                continue;
            }
            normalized.add(network.trim().toUpperCase());
        }
        return normalized;
    }
}


