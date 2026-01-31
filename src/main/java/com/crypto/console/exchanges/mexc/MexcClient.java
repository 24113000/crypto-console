package com.crypto.console.exchanges.mexc;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class MexcClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public MexcClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("mexc", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for mexc");
        }

        long timestamp = System.currentTimeMillis();
        String query = "timestamp=" + timestamp + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/api/v3/account?" + query + "&signature=" + signature;

        JsonNode response = getJson(uri, apiKey);

        if (response == null || response.get("balances") == null || !response.get("balances").isArray()) {
            throw new ExchangeException("Unexpected response from MEXC account API");
        }

        Iterator<JsonNode> it = response.get("balances").elements();
        while (it.hasNext()) {
            JsonNode balance = it.next();
            String balanceAsset = balance.hasNonNull("asset") ? balance.get("asset").asText() : null;
            if (balanceAsset != null && balanceAsset.equalsIgnoreCase(asset)) {
                BigDecimal free = toDecimal(balance.get("free"));
                BigDecimal locked = toDecimal(balance.get("locked"));
                return new Balance(balanceAsset, free, locked);
            }
        }

        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /api/v3/capital/config/getall (signed)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /api/v3/depth (public)");
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        throw notImplemented("POST /api/v3/order (signed)");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        throw notImplemented("POST /api/v3/order (signed)");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        throw notImplemented("POST /api/v3/capital/withdraw/apply (signed)");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("GET /api/v3/time (public)");
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for mexc");
        }

        Set<String> networks = new HashSet<>();

        // Prefer deposit address endpoint for a single coin to avoid large config responses.
        long timestamp = System.currentTimeMillis();
        String query = "coin=" + asset.toUpperCase() + "&timestamp=" + timestamp + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/api/v3/capital/deposit/address?" + query + "&signature=" + signature;
        JsonNode addressResponse = getJson(uri, apiKey);
        if (addressResponse != null && addressResponse.isArray()) {
            for (JsonNode item : addressResponse) {
                String net = item.hasNonNull("network") ? item.get("network").asText() : null;
                if (StringUtils.isNotBlank(net)) {
                    networks.add(net.trim().toUpperCase());
                }
            }
        }

        if (!networks.isEmpty()) {
            return networks;
        }

        // Fallback to config/getall if deposit address list is empty.
        long ts2 = System.currentTimeMillis();
        String query2 = "timestamp=" + ts2 + "&recvWindow=5000";
        String signature2 = sign(query2, apiSecret);
        String uri2 = "/api/v3/capital/config/getall?" + query2 + "&signature=" + signature2;
        JsonNode response = getJson(uri2, apiKey);
        if (response == null || !response.isArray()) {
            throw new ExchangeException("Unexpected response from MEXC config API");
        }

        for (JsonNode coin : response) {
            String coinName = coin.hasNonNull("coin") ? coin.get("coin").asText() : null;
            if (coinName == null || !coinName.equalsIgnoreCase(asset)) {
                continue;
            }
            JsonNode networkList = coin.get("networkList");
            if (networkList != null && networkList.isArray()) {
                for (JsonNode network : networkList) {
                    String name = network.hasNonNull("netWork") ? network.get("netWork").asText()
                            : (network.hasNonNull("network") ? network.get("network").asText() : null);
                    if (StringUtils.isNotBlank(name)) {
                        networks.add(name.trim().toUpperCase());
                    }
                }
            }
        }
        return networks;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for mexc");
        }

        String normalizedNetwork = normalizeNetwork(network);
        for (String candidate : networkCandidates(network, normalizedNetwork)) {
            String address = getDepositAddressGet(asset, candidate, apiKey, apiSecret, true);
            if (StringUtils.isNotBlank(address)) {
                return address;
            }
        }
        return getDepositAddressGet(asset, normalizedNetwork, apiKey, apiSecret, false);
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, false, false, false, false, true);
    }

    private String sign(String data, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign MEXC request", e);
        }
    }

    private BigDecimal toDecimal(JsonNode node) {
        if (node == null || node.isNull()) {
            return BigDecimal.ZERO;
        }
        String text = node.asText();
        if (StringUtils.isBlank(text)) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(text);
    }

    private JsonNode getJson(String uri, String apiKey) {
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MEXC-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(String.class)
                .block();
        if (StringUtils.isBlank(body)) {
            throw new ExchangeException("Empty response from MEXC");
        }
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse MEXC response", e);
        }
    }

    private String getDepositAddressGet(String asset, String network, String apiKey, String apiSecret, boolean includeNetworkParam) {
        long timestamp = System.currentTimeMillis();
        StringBuilder query = new StringBuilder();
        query.append("coin=").append(asset.toUpperCase());
        if (includeNetworkParam && StringUtils.isNotBlank(network)) {
            query.append("&network=").append(network);
        }
        query.append("&timestamp=").append(timestamp);
        query.append("&recvWindow=5000");

        String signature = sign(query.toString(), apiSecret);
        String uri = "/api/v3/capital/deposit/address?" + query + "&signature=" + signature;
        JsonNode response = getJson(uri, apiKey);
        if (response == null) {
            return null;
        }

        if (response.isArray()) {
            for (JsonNode item : response) {
                String net = item.hasNonNull("network") ? item.get("network").asText() : null;
                if (StringUtils.isBlank(network) || networksMatch(net, network)) {
                    JsonNode addr = item.get("address");
                    if (addr != null && StringUtils.isNotBlank(addr.asText())) {
                        return addr.asText();
                    }
                }
            }
            return null;
        }

        JsonNode addr = response.get("address");
        return addr == null ? null : addr.asText();
    }

    private boolean networksMatch(String a, String b) {
        if (StringUtils.isBlank(a) || StringUtils.isBlank(b)) {
            return false;
        }
        String na = normalizeNetwork(a);
        String nb = normalizeNetwork(b);
        if (na.equalsIgnoreCase(nb)) {
            return true;
        }
        if ("TRX".equalsIgnoreCase(nb) && "TRC20".equalsIgnoreCase(na)) {
            return true;
        }
        if ("BSC".equalsIgnoreCase(nb) && na.contains("BEP20")) {
            return true;
        }
        return na.contains(nb) || nb.contains(na);
    }

    private String normalizeNetwork(String network) {
        if (StringUtils.isBlank(network)) {
            return null;
        }
        String cleaned = network.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        if ("TRX".equalsIgnoreCase(cleaned)) {
            return "TRC20";
        }
        return cleaned;
    }

    private java.util.List<String> networkCandidates(String raw, String normalized) {
        java.util.LinkedHashSet<String> candidates = new java.util.LinkedHashSet<>();
        if (StringUtils.isNotBlank(raw)) {
            candidates.add(raw.trim());
        }
        if (StringUtils.isNotBlank(normalized)) {
            candidates.add(normalized);
        }
        if ("BSC".equalsIgnoreCase(normalized) || "BEP20".equalsIgnoreCase(normalized)) {
            candidates.add("BEP20(BSC)");
        }
        if ("TRC20".equalsIgnoreCase(normalized) || "TRX".equalsIgnoreCase(normalized)) {
            candidates.add("TRC20");
        }
        return new java.util.ArrayList<>(candidates);
    }
}
