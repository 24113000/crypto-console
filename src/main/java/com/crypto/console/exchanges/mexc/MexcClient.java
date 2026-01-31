package com.crypto.console.exchanges.mexc;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.util.LogSanitizer;
import lombok.extern.slf4j.Slf4j;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

@Slf4j
public class MexcClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
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

        long timestamp = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("timestamp", String.valueOf(timestamp));
        params.put("recvWindow", "5000");
        String query = signQuery(params, apiSecret);
        String uri = "/api/v3/account?" + query;

        LOG.info("mexc GET {}", LogSanitizer.sanitize(uri));
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
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for mexc");
        }

        String symbol = (base + quote).toUpperCase();
        long timestamp = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        params.put("quoteOrderQty", quoteAmount.toPlainString());
        params.put("timestamp", String.valueOf(timestamp));
        params.put("recvWindow", "5000");

        String query = signQuery(params, apiSecret);
        String uri = "/api/v3/order?" + query;
        LOG.info("mexc POST {}", LogSanitizer.sanitize(uri));

        JsonNode response = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MEXC-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "MEXC order failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();

        if (response == null || response.get("orderId") == null) {
            throw new ExchangeException("Unexpected response from MEXC order API");
        }
        String orderId = response.get("orderId").asText();
        return new OrderResult(orderId, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for mexc");
        }

        String symbol = (base + quote).toUpperCase();
        long timestamp = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "MARKET");
        params.put("quantity", baseAmount.toPlainString());
        params.put("timestamp", String.valueOf(timestamp));
        params.put("recvWindow", "5000");

        String query = signQuery(params, apiSecret);
        String uri = "/api/v3/order?" + query;
        LOG.info("mexc POST {}", LogSanitizer.sanitize(uri));

        JsonNode response = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MEXC-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "MEXC order failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();

        if (response == null || response.get("orderId") == null) {
            throw new ExchangeException("Unexpected response from MEXC order API");
        }
        String orderId = response.get("orderId").asText();
        return new OrderResult(orderId, "SUBMITTED", "market sell submitted");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        if (amount == null || amount.signum() <= 0) {
            throw new ExchangeException("Amount must be positive");
        }
        if (StringUtils.isBlank(address)) {
            throw new ExchangeException("Address is required");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for mexc");
        }

        try {
            return doWithdraw(true, false, "netWork", asset, amount, network, address, memoOrNull, apiKey, apiSecret);
        } catch (ExchangeException e) {
            String msg = e.getMessage();
            if (msg != null && (msg.contains("\"code\":10232") || msg.contains("\"code\":700004"))) {
                // Some MEXC accounts expect both "coin" and "currency"
                try {
                    return doWithdraw(true, true, "netWork", asset, amount, network, address, memoOrNull, apiKey, apiSecret);
                } catch (ExchangeException e2) {
                    String msg2 = e2.getMessage();
                    if (msg2 != null && msg2.contains("\"code\":10232")) {
                        // Fallback to "chain" parameter name
                        return doWithdraw(true, true, "chain", asset, amount, network, address, memoOrNull, apiKey, apiSecret);
                    }
                    throw e2;
                }
            }
            throw e;
        }
    }

    private WithdrawResult doWithdraw(boolean includeCoin, boolean includeCurrency, String networkKey, String asset, BigDecimal amount, String network,
                                      String address, String memoOrNull, String apiKey, String apiSecret) {
        long timestamp = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        if (includeCoin) {
            params.put("coin", asset.toUpperCase());
        }
        if (includeCurrency) {
            params.put("currency", asset.toUpperCase());
        }
        params.put("address", address);
        params.put("amount", amount.toPlainString());
        String normalizedNetwork = normalizeDepositNetwork(network);
        if (StringUtils.isNotBlank(normalizedNetwork)) {
            params.put(networkKey, normalizedNetwork);
        }
        if (StringUtils.isNotBlank(memoOrNull)) {
            params.put("memo", memoOrNull);
        }
        params.put("timestamp", String.valueOf(timestamp));
        params.put("recvWindow", "5000");

        String query = signQuery(params, apiSecret);
        String uri = "/api/v3/capital/withdraw?" + query;
        LOG.info("mexc POST {}", LogSanitizer.sanitize(uri));

        JsonNode response = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MEXC-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "MEXC withdraw failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();

        if (response == null) {
            throw new ExchangeException("Unexpected response from MEXC withdraw API");
        }
        String id = response.hasNonNull("id") ? response.get("id").asText() : null;
        if (StringUtils.isBlank(id)) {
            id = response.hasNonNull("withdrawId") ? response.get("withdrawId").asText() : null;
        }
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from MEXC withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
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
        long timestamp = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("coin", asset.toUpperCase());
        params.put("timestamp", String.valueOf(timestamp));
        params.put("recvWindow", "5000");
        String query = signQuery(params, apiSecret);
        String uri = "/api/v3/capital/deposit/address?" + query;
        LOG.info("mexc GET {}", LogSanitizer.sanitize(uri));
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
        long ts2 = getServerTime();
        Map<String, String> params2 = new LinkedHashMap<>();
        params2.put("timestamp", String.valueOf(ts2));
        params2.put("recvWindow", "5000");
        String query2 = signQuery(params2, apiSecret);
        String uri2 = "/api/v3/capital/config/getall?" + query2;
        LOG.info("mexc GET {}", LogSanitizer.sanitize(uri2));
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

        String normalizedNetwork = normalizeDepositNetwork(network);
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
        return new ExchangeCapabilities(true, false, false, true, false, false, true);
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

    private String signQuery(Map<String, String> params, String secret) {
        Map<String, String> sorted = new java.util.TreeMap<>(params);
        String qs = buildQueryString(sorted);
        String signature = sign(qs, secret);
        return qs + "&signature=" + signature;
    }

    private String buildQueryString(Map<String, String> params) {
        StringBuilder qs = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (!qs.isEmpty()) {
                qs.append("&");
            }
            qs.append(encodeQuery(entry.getKey()))
              .append("=")
              .append(encodeQuery(entry.getValue()));
        }
        return qs.toString();
    }

    private String encodeQuery(String value) {
        try {
            String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
            return encoded.replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode MEXC query parameter", e);
        }
    }
    
    private long getServerTime() {
        try {
            JsonNode resp = webClient.get()
                    .uri("/api/v3/time")
                    .header(HttpHeaders.USER_AGENT, "crypto-console")
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();
            if (resp != null && resp.hasNonNull("serverTime")) {
                return resp.get("serverTime").asLong();
            }
        } catch (Exception ignored) {
        }
        return System.currentTimeMillis();
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
        LOG.info("mexc GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MEXC-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "MEXC request failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
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
        long timestamp = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("coin", asset.toUpperCase());
        if (includeNetworkParam && StringUtils.isNotBlank(network)) {
            params.put("network", network);
        }
        params.put("timestamp", String.valueOf(timestamp));
        params.put("recvWindow", "5000");

        String query = signQuery(params, apiSecret);
        String uri = "/api/v3/capital/deposit/address?" + query;
        LOG.info("mexc GET {}", LogSanitizer.sanitize(uri));
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
        String na = normalizeDepositNetwork(a);
        String nb = normalizeDepositNetwork(b);
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

    @Override
    public String normalizeDepositNetwork(String network) {
        if (StringUtils.isBlank(network)) {
            return null;
        }
        String cleaned = network.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        return switch (cleaned) {
            case "ARBITRUMONE", "ARB" -> "ARBITRUM";
            case "AVALANCHECCHAIN", "AVAXCCHAIN", "AVAXC" -> "AVAXC";
            case "BNBSMARTCHAIN", "BSC", "BEP20" -> "BSC";
            case "ETHEREUM", "ERC20", "ETH" -> "ERC20";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL" -> "SOL";
            case "TRON", "TRC20", "TRX" -> "TRC20";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "KAIA" -> "KAIA";
            case "CELO" -> "CELO";
            case "HECO" -> "HECO";
            case "PLASMA" -> "PLASMA";
            default -> cleaned;
        };
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
