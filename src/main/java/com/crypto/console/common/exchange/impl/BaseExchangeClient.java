package com.crypto.console.common.exchange.impl;

import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.model.ExchangeCapabilities;
import com.crypto.console.common.model.ExchangeException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.ExchangeStrategies;

public abstract class BaseExchangeClient implements ExchangeClient {
    protected final String name;
    protected final String baseUrl;
    protected final SecretsProperties.ExchangeSecrets secrets;
    protected final WebClient webClient;

    protected BaseExchangeClient(String name, AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        this.name = name;
        this.baseUrl = cfg == null ? null : cfg.getBaseUrl();
        this.secrets = secrets;
        if (StringUtils.isBlank(this.baseUrl)) {
            throw new IllegalStateException("Missing baseUrl for exchange: " + name);
        }
        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                .build();
        this.webClient = WebClient.builder()
                .baseUrl(this.baseUrl)
                .exchangeStrategies(strategies)
                .build();
    }

    @Override
    public String name() {
        return name;
    }

    protected ExchangeException notImplemented(String endpointNote) {
        return new ExchangeException("Not implemented yet: verify endpoint for " + name + " - " + endpointNote);
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(false, false, false, false, false, false, false);
    }
}

