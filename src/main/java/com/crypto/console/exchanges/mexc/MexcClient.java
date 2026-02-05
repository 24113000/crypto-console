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
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String symbol = resolveSymbol(base, quote);
        if (symbol == null) {
            throw new ExchangeException("Invalid symbol: " + (base + quote).toUpperCase() + ". Check base/quote assets.");
        }

        String uri = "/api/v3/depth?symbol=" + symbol + "&limit=1000";
        LOG.info("mexc GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        if (response == null || response.get("asks") == null || !response.get("asks").isArray()) {
            throw new ExchangeException("Unexpected response from MEXC depth API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal spentQuote = BigDecimal.ZERO;
        BigDecimal boughtBase = BigDecimal.ZERO;
        List<BuyInfoItem> affectedItems = new ArrayList<>();

        for (JsonNode ask : response.get("asks")) {
            if (ask == null || !ask.isArray() || ask.size() < 2) {
                continue;
            }
            BigDecimal price = toDecimal(ask.get(0));
            BigDecimal quantity = toDecimal(ask.get(1));
            if (price.signum() <= 0 || quantity.signum() <= 0) {
                continue;
            }

            BigDecimal levelQuoteCost = price.multiply(quantity);
            if (remainingQuote.compareTo(levelQuoteCost) >= 0) {
                boughtBase = boughtBase.add(quantity);
                spentQuote = spentQuote.add(levelQuoteCost);
                affectedItems.add(new BuyInfoItem(price, quantity, levelQuoteCost));
                remainingQuote = remainingQuote.subtract(levelQuoteCost);
            } else {
                BigDecimal partialQty = remainingQuote.divide(price, 18, RoundingMode.DOWN);
                if (partialQty.signum() > 0) {
                    BigDecimal partialCost = partialQty.multiply(price);
                    boughtBase = boughtBase.add(partialQty);
                    spentQuote = spentQuote.add(partialCost);
                    affectedItems.add(new BuyInfoItem(price, partialQty, partialCost));
                }
                remainingQuote = BigDecimal.ZERO;
                break;
            }
            if (remainingQuote.signum() == 0) {
                break;
            }
        }

        if (boughtBase.signum() <= 0) {
            throw new ExchangeException("No ask liquidity available for " + symbol);
        }

        BigDecimal averagePrice = spentQuote.divide(boughtBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol, quoteAmount, spentQuote, boughtBase, averagePrice, List.copyOf(affectedItems));
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
                return doWithdraw(true, true, "netWork", asset, amount, network, address, memoOrNull, apiKey, apiSecret);
            }
            throw e;
        }
    }

    private WithdrawResult doWithdraw(boolean includeCoin, boolean includeCurrency, String networkKey, String asset, BigDecimal amount, String network,
                                      String address, String memoOrNull, String apiKey, String apiSecret) {
        long timestamp = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        ResolvedWithdraw resolved = resolveWithdrawCoinAndNetwork(asset, network, apiKey, apiSecret);
        if (includeCoin) {
            params.put("coin", resolved.coin);
        }
        if (includeCurrency) {
            params.put("currency", resolved.coin);
        }
        params.put("address", address);
        params.put("amount", amount.toPlainString());
        String normalizedNetwork = resolveWithdrawNetwork(resolved, network);
        if (StringUtils.isBlank(normalizedNetwork)) {
            throw new ExchangeException("MEXC withdraw requires network (netWork) parameter");
        }
        params.put(networkKey, normalizedNetwork);
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

    private ResolvedWithdraw resolveWithdrawCoinAndNetwork(String asset, String network, String apiKey, String apiSecret) {
        String assetUpper = asset.toUpperCase();
        long ts = getServerTime();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("timestamp", String.valueOf(ts));
        params.put("recvWindow", "5000");
        String query = signQuery(params, apiSecret);
        String uri = "/api/v3/capital/config/getall?" + query;
        LOG.info("mexc GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = getJson(uri, apiKey);
        if (response == null || !response.isArray()) {
            throw new ExchangeException("Unexpected response from MEXC config API");
        }

        String normalizedTarget = normalizeDepositNetwork(network);
        for (JsonNode coin : response) {
            String coinName = coin.hasNonNull("coin") ? coin.get("coin").asText() : null;
            if (coinName == null || !coinName.equalsIgnoreCase(assetUpper)) {
                continue;
            }
            String resolvedNetwork = null;
            JsonNode networkList = coin.get("networkList");
            if (networkList != null && networkList.isArray()) {
                for (JsonNode net : networkList) {
                    String name = net.hasNonNull("netWork") ? net.get("netWork").asText()
                            : (net.hasNonNull("network") ? net.get("network").asText() : null);
                    if (StringUtils.isBlank(name)) {
                        continue;
                    }
                    if (StringUtils.isBlank(normalizedTarget)) {
                        resolvedNetwork = name;
                        break;
                    }
                    if (normalizeDepositNetwork(name).equalsIgnoreCase(normalizedTarget)) {
                        resolvedNetwork = name;
                        break;
                    }
                }
            }
            return new ResolvedWithdraw(coinName.toUpperCase(), resolvedNetwork);
        }

        throw new ExchangeException("MEXC does not support withdrawals for asset: " + assetUpper);
    }

    private String resolveWithdrawNetwork(ResolvedWithdraw resolved, String network) {
        if (StringUtils.isNotBlank(resolved.network)) {
            return resolved.network;
        }
        return normalizeDepositNetwork(network);
    }

    private record ResolvedWithdraw(String coin, String network) {
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

    private String resolveSymbol(String base, String quote) {
        String candidate = (base + quote).toUpperCase();
        LOG.info("mexc GET {}", LogSanitizer.sanitize("/api/v3/exchangeInfo?symbol=" + candidate));
        JsonNode direct = webClient.get()
                .uri("/api/v3/exchangeInfo?symbol=" + candidate)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> reactor.core.publisher.Mono.empty())
                .block();
        if (direct != null && direct.has("symbols") && direct.get("symbols").isArray()) {
            JsonNode first = direct.get("symbols").size() > 0 ? direct.get("symbols").get(0) : null;
            if (first != null && first.hasNonNull("symbol")) {
                return first.get("symbol").asText();
            }
        }

        LOG.info("mexc GET /api/v3/exchangeInfo");
        JsonNode full = webClient.get()
                .uri("/api/v3/exchangeInfo")
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> reactor.core.publisher.Mono.empty())
                .block();
        if (full == null || !full.has("symbols") || !full.get("symbols").isArray()) {
            return null;
        }
        for (JsonNode symbolNode : full.get("symbols")) {
            String baseAsset = symbolNode.hasNonNull("baseAsset") ? symbolNode.get("baseAsset").asText() : null;
            String quoteAsset = symbolNode.hasNonNull("quoteAsset") ? symbolNode.get("quoteAsset").asText() : null;
            if (baseAsset != null && quoteAsset != null
                    && baseAsset.equalsIgnoreCase(base)
                    && quoteAsset.equalsIgnoreCase(quote)) {
                return symbolNode.get("symbol").asText();
            }
        }
        return null;
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
        String candidate = network;
        int openIdx = candidate.indexOf('(');
        int closeIdx = candidate.indexOf(')');
        if (openIdx >= 0 && closeIdx > openIdx) {
            candidate = candidate.substring(openIdx + 1, closeIdx);
        }
        String cleaned = candidate.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        return switch (cleaned) {
            case "ARBITRUMONE", "ARB" -> "ARBITRUM";
            case "AVALANCHECCHAIN", "AVAXCCHAIN", "AVAXC" -> "AVAXC";
            case "BNBSMARTCHAIN", "BSC", "BEP20" -> "BSC";
            case "ETHEREUM", "ERC20", "ETH" -> "ERC20";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL", "SOLANASOL" -> "SOL";
            case "TRON", "TRC20", "TRX", "TRONTRC20" -> "TRC20";
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
