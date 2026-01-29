package com.crypto.console.common.exchange.impl;

import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.exchange.ExchangeName;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.exchanges.binance.BinanceClient;

import java.util.EnumMap;
import java.util.Map;

import static com.crypto.console.common.exchange.ExchangeName.BINANCE;
import static com.crypto.console.common.exchange.ExchangeName.BITMART;
import static com.crypto.console.common.exchange.ExchangeName.COINEX;
import static com.crypto.console.common.exchange.ExchangeName.GATEIO;
import static com.crypto.console.common.exchange.ExchangeName.KUCOIN;
import static com.crypto.console.common.exchange.ExchangeName.MEXC;

public class ExchangeRegistry {
    private final Map<ExchangeName, ExchangeClient> clients;
    private final Map<ExchangeName, SecretsProperties.ExchangeSecrets> secrets;

    private ExchangeRegistry(Map<ExchangeName, ExchangeClient> clients,
                             Map<ExchangeName, SecretsProperties.ExchangeSecrets> secrets) {
        this.clients = clients;
        this.secrets = secrets;
    }

    public static ExchangeRegistry create(AppProperties appProperties, SecretsProperties secretsProperties) {
        Map<ExchangeName, ExchangeClient> map = new EnumMap<>(ExchangeName.class);
        Map<ExchangeName, SecretsProperties.ExchangeSecrets> secrets = new EnumMap<>(ExchangeName.class);
        if (secretsProperties != null && secretsProperties.getExchanges() != null) {
            secretsProperties.getExchanges().forEach((key, value) -> secrets.put(ExchangeName.from(key), value));
        }

        createClient(BINANCE,   appProperties, map, secrets);
        createClient(MEXC,      appProperties, map, secrets);
        createClient(COINEX,    appProperties, map, secrets);
        createClient(BITMART,   appProperties, map, secrets);
        createClient(KUCOIN,    appProperties, map, secrets);
        createClient(GATEIO,    appProperties, map, secrets);

        return new ExchangeRegistry(map, secrets);
    }

    public ExchangeClient getClient(String exchange) {
        ExchangeClient client = clients.get(ExchangeName.from(exchange));
        if (client == null) {
            throw new ExchangeException("Unsupported exchange: " + exchange);
        }
        return client;
    }

    public boolean hasSecrets(String exchange) {
        return secrets.containsKey(ExchangeName.from(exchange));
    }

    private static void createClient(ExchangeName exchange,
                                     AppProperties appProperties,
                                     Map<ExchangeName, ExchangeClient> map,
                                     Map<ExchangeName, SecretsProperties.ExchangeSecrets> secrets) {
        map.put(exchange, new BinanceClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
    }
}

