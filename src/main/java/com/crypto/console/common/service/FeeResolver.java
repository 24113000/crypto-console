package com.crypto.console.common.service;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.model.WithdrawalFees;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class FeeResolver {
    private final AppProperties config;

    public FeeResolver(AppProperties config) {
        this.config = config;
    }

    public WithdrawalFees resolveWithdrawalFees(ExchangeClient client, String exchange, String asset) {
        try {
            if (client.capabilities().supportsWithdrawalFees) {
                WithdrawalFees apiFees = client.getWithdrawalFees(asset);
                if (apiFees != null && apiFees.feeByNetwork != null && !apiFees.feeByNetwork.isEmpty()) {
                    return apiFees;
                }
            }
        } catch (Exception ignored) {
        }

        Map<String, Map<String, String>> exchangeFees = config.getWithdrawFeesFallback() == null
                ? null
                : config.getWithdrawFeesFallback().get(exchange);
        if (exchangeFees == null) {
            throw new ExchangeException("No withdrawal fee data for exchange: " + exchange);
        }
        Map<String, String> assetFees = exchangeFees.get(asset);
        if (assetFees == null || assetFees.isEmpty()) {
            throw new ExchangeException("No withdrawal fee data for " + exchange + " " + asset);
        }
        Map<String, BigDecimal> parsed = new HashMap<>();
        for (Map.Entry<String, String> entry : assetFees.entrySet()) {
            if (StringUtils.isBlank(entry.getValue())) {
                continue;
            }
            parsed.put(entry.getKey(), new BigDecimal(entry.getValue()));
        }
        if (parsed.isEmpty()) {
            throw new ExchangeException("No valid withdrawal fee values for " + exchange + " " + asset);
        }
        return new WithdrawalFees(asset, parsed);
    }
}


