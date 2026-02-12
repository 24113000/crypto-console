package com.crypto.console.common.exchange.impl;

import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.exchange.ExchangeName;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.exchanges.binance.BinanceClient;
import com.crypto.console.exchanges.bingx.BingxClient;
import com.crypto.console.exchanges.bitget.BitgetClient;
import com.crypto.console.exchanges.bitmart.BitMartClient;
import com.crypto.console.exchanges.bitrue.BitrueClient;
import com.crypto.console.exchanges.coinex.CoinExClient;
import com.crypto.console.exchanges.exstub1.ExStub1Client;
import com.crypto.console.exchanges.exstub2.ExStub2Client;
import com.crypto.console.exchanges.gateio.GateIoClient;
import com.crypto.console.exchanges.hitbtc.HitBtcClient;
import com.crypto.console.exchanges.htx.HtxClient;
import com.crypto.console.exchanges.kucoin.KuCoinClient;
import com.crypto.console.exchanges.ascendex.AscendExClient;
import com.crypto.console.exchanges.lbank.LBankClient;
import com.crypto.console.exchanges.mexc.MexcClient;
import com.crypto.console.exchanges.poloniex.PoloniexClient;
import com.crypto.console.exchanges.xt.XtClient;

import java.util.EnumMap;
import java.util.Map;

import static com.crypto.console.common.exchange.ExchangeName.BINANCE;
import static com.crypto.console.common.exchange.ExchangeName.BINGX;
import static com.crypto.console.common.exchange.ExchangeName.BITGET;
import static com.crypto.console.common.exchange.ExchangeName.BITMART;
import static com.crypto.console.common.exchange.ExchangeName.BITRUE;
import static com.crypto.console.common.exchange.ExchangeName.COINEX;
import static com.crypto.console.common.exchange.ExchangeName.EXSTUB1;
import static com.crypto.console.common.exchange.ExchangeName.EXSTUB2;
import static com.crypto.console.common.exchange.ExchangeName.GATEIO;
import static com.crypto.console.common.exchange.ExchangeName.HITBTC;
import static com.crypto.console.common.exchange.ExchangeName.HTX;
import static com.crypto.console.common.exchange.ExchangeName.KUCOIN;
import static com.crypto.console.common.exchange.ExchangeName.ASCENDEX;
import static com.crypto.console.common.exchange.ExchangeName.LBANK;
import static com.crypto.console.common.exchange.ExchangeName.MEXC;
import static com.crypto.console.common.exchange.ExchangeName.POLONIEX;
import static com.crypto.console.common.exchange.ExchangeName.XT;

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
        createClient(BINGX,     appProperties, map, secrets);
        createClient(BITGET,    appProperties, map, secrets);
        createClient(BITRUE,    appProperties, map, secrets);
        createClient(MEXC,      appProperties, map, secrets);
        createClient(XT,        appProperties, map, secrets);
        createClient(COINEX,    appProperties, map, secrets);
        createClient(BITMART,   appProperties, map, secrets);
        createClient(KUCOIN,    appProperties, map, secrets);
        createClient(GATEIO,    appProperties, map, secrets);
        createClient(HITBTC,    appProperties, map, secrets);
        createClient(HTX,       appProperties, map, secrets);
        createClient(LBANK,     appProperties, map, secrets);
        createClient(ASCENDEX,  appProperties, map, secrets);
        createClient(POLONIEX,  appProperties, map, secrets);
        createClient(EXSTUB1,   appProperties, map, secrets);
        createClient(EXSTUB2,   appProperties, map, secrets);

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
        switch (exchange) {
            case BINANCE ->
                    map.put(exchange, new BinanceClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case BINGX ->
                    map.put(exchange, new BingxClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case BITGET ->
                    map.put(exchange, new BitgetClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case BITRUE ->
                    map.put(exchange, new BitrueClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case MEXC ->
                    map.put(exchange, new MexcClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case XT ->
                    map.put(exchange, new XtClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case COINEX ->
                    map.put(exchange, new CoinExClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case BITMART ->
                    map.put(exchange, new BitMartClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case KUCOIN ->
                    map.put(exchange, new KuCoinClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case GATEIO ->
                    map.put(exchange, new GateIoClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case HITBTC ->
                    map.put(exchange, new HitBtcClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case HTX ->
                    map.put(exchange, new HtxClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case LBANK ->
                    map.put(exchange, new LBankClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case ASCENDEX ->
                    map.put(exchange, new AscendExClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case POLONIEX ->
                    map.put(exchange, new PoloniexClient(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case EXSTUB1 ->
                    map.put(exchange, new ExStub1Client(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
            case EXSTUB2 ->
                    map.put(exchange, new ExStub2Client(appProperties.getExchanges().get(exchange.id()), secrets.get(exchange)));
        }
    }
}
