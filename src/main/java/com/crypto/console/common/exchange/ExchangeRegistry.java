package com.crypto.console.common.exchange;

import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.exchanges.binance.BinanceClient;
import com.crypto.console.exchanges.bitmart.BitMartClient;
import com.crypto.console.exchanges.coinex.CoinExClient;
import com.crypto.console.exchanges.gateio.GateIoClient;
import com.crypto.console.exchanges.kucoin.KuCoinClient;
import com.crypto.console.exchanges.mexc.MexcClient;
import com.crypto.console.exchanges.xt.XtClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ExchangeRegistry {
    private final Map<String, ExchangeClient> clients;
    private final Map<String, SecretsProperties.ExchangeSecrets> secrets;

    private ExchangeRegistry(Map<String, ExchangeClient> clients, Map<String, SecretsProperties.ExchangeSecrets> secrets) {
        this.clients = clients;
        this.secrets = secrets;
    }

    public static ExchangeRegistry create(AppProperties appProperties, SecretsProperties secretsProperties) {
        Map<String, ExchangeClient> map = new HashMap<>();
        Map<String, SecretsProperties.ExchangeSecrets> secrets = secretsProperties == null || secretsProperties.getExchanges() == null
                ? Collections.emptyMap()
                : secretsProperties.getExchanges();

        map.put("binance", new BinanceClient(appProperties.getExchanges().get("binance"), secrets.get("binance")));
        map.put("mexc", new MexcClient(appProperties.getExchanges().get("mexc"), secrets.get("mexc")));
        map.put("xt", new XtClient(appProperties.getExchanges().get("xt"), secrets.get("xt")));
        map.put("coinex", new CoinExClient(appProperties.getExchanges().get("coinex"), secrets.get("coinex")));
        map.put("bitmart", new BitMartClient(appProperties.getExchanges().get("bitmart"), secrets.get("bitmart")));
        map.put("kucoin", new KuCoinClient(appProperties.getExchanges().get("kucoin"), secrets.get("kucoin")));
        map.put("gateio", new GateIoClient(appProperties.getExchanges().get("gateio"), secrets.get("gateio")));

        return new ExchangeRegistry(map, secrets);
    }

    public ExchangeClient getClient(String exchange) {
        ExchangeClient client = clients.get(exchange.toLowerCase());
        if (client == null) {
            throw new ExchangeException("Unsupported exchange: " + exchange);
        }
        return client;
    }

    public boolean hasSecrets(String exchange) {
        return secrets.containsKey(exchange.toLowerCase());
    }
}


