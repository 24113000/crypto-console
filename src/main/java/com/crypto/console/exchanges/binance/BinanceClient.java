package com.crypto.console.exchanges.binance;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;
import com.crypto.console.common.model.ExchangeException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class BinanceClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider {
    public BinanceClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("binance", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for binance");
        }

        long timestamp = System.currentTimeMillis();
        String query = "timestamp=" + timestamp + "&recvWindow=5000&asset=" + asset.toUpperCase();
        String signature = sign(query, apiSecret);
        String uri = "/sapi/v1/asset/get-funding-asset?" + query + "&signature=" + signature;

        JsonNode response = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        if (response == null || !response.isArray()) {
            throw new ExchangeException("Unexpected response from Binance funding API");
        }

        Iterator<JsonNode> it = response.elements();
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
        throw notImplemented("GET /sapi/v1/capital/config/getall (signed)");
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
            throw new ExchangeException("Missing API credentials for binance");
        }

        // 1) Move quote asset from Funding to Spot.
        transferFundingToSpot(apiKey, apiSecret, quote, quoteAmount);

        // 2) Resolve symbol and place market buy using quoteOrderQty.
        String symbol = resolveSymbol(base, quote);
        if (symbol == null) {
            throw new ExchangeException("Invalid symbol: " + (base + quote).toUpperCase() + ". Check base/quote assets.");
        }
        long ts = System.currentTimeMillis();
        String orderQuery = "symbol=" + symbol
                + "&side=BUY"
                + "&type=MARKET"
                + "&quoteOrderQty=" + quoteAmount.toPlainString()
                + "&newOrderRespType=RESULT"
                + "&timestamp=" + ts
                + "&recvWindow=5000";
        String orderSignature = sign(orderQuery, apiSecret);
        String orderUri = "/api/v3/order?" + orderQuery + "&signature=" + orderSignature;

        JsonNode orderResp = webClient.post()
                .uri(orderUri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "Binance order failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();

        if (orderResp == null || orderResp.get("orderId") == null) {
            throw new ExchangeException("Unexpected response from Binance order API");
        }

        long orderId = orderResp.get("orderId").asLong();
        String status = orderResp.hasNonNull("status") ? orderResp.get("status").asText() : "NEW";
        BigDecimal executedQty = toDecimal(orderResp.get("executedQty"));

        // 3) Wait until filled.
        if (!"FILLED".equalsIgnoreCase(status)) {
            OrderStatusResult finalStatus = pollOrderStatus(apiKey, apiSecret, symbol, orderId);
            status = finalStatus.status;
            executedQty = finalStatus.executedQty;
        }

        if (!"FILLED".equalsIgnoreCase(status)) {
            return new OrderResult(String.valueOf(orderId), status, "Order not filled");
        }

        if (executedQty.signum() <= 0) {
            throw new ExchangeException("Order filled but executed quantity is zero");
        }

        // 4) Move bought base asset from Spot to Funding (use actual spot free balance).
        BigDecimal spotFree = getSpotFreeBalance(apiKey, apiSecret, base);
        if (spotFree.signum() > 0) {
            transferSpotToFunding(apiKey, apiSecret, base, spotFree);
        }

        return new OrderResult(String.valueOf(orderId), status, "filledQty=" + executedQty);
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        throw notImplemented("POST /api/v3/order (signed)");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        throw notImplemented("POST /sapi/v1/capital/withdraw/apply (signed)");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("GET /api/v3/time (public)");
    }

    @Override
    public java.util.Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for binance");
        }

        long timestamp = System.currentTimeMillis();
        String query = "timestamp=" + timestamp + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/sapi/v1/capital/config/getall?" + query + "&signature=" + signature;

        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        if (response == null || !response.isArray()) {
            throw new ExchangeException("Unexpected response from Binance config API");
        }

        java.util.Set<String> networks = new java.util.HashSet<>();
        for (JsonNode coin : response) {
            String coinName = coin.hasNonNull("coin") ? coin.get("coin").asText() : null;
            if (coinName == null || !coinName.equalsIgnoreCase(asset)) {
                continue;
            }
            JsonNode networkList = coin.get("networkList");
            if (networkList != null && networkList.isArray()) {
                for (JsonNode network : networkList) {
                    String name = network.hasNonNull("network") ? network.get("network").asText() : null;
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
            throw new ExchangeException("Missing API credentials for binance");
        }

        long timestamp = System.currentTimeMillis();
        StringBuilder query = new StringBuilder();
        query.append("coin=").append(asset.toUpperCase());
        if (StringUtils.isNotBlank(network)) {
            query.append("&network=").append(network.toUpperCase());
        }
        query.append("&timestamp=").append(timestamp);
        query.append("&recvWindow=5000");

        String signature = sign(query.toString(), apiSecret);
        String uri = "/sapi/v1/capital/deposit/address?" + query + "&signature=" + signature;

        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        if (response == null) {
            throw new ExchangeException("Unexpected response from Binance deposit address API");
        }
        JsonNode addressNode = response.get("address");
        return addressNode == null ? null : addressNode.asText();
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, true, true, true, true, false, true);
    }

    private void transferFundingToSpot(String apiKey, String apiSecret, String asset, BigDecimal amount) {
        transfer(apiKey, apiSecret, "FUNDING_MAIN", asset, amount);
    }

    private void transferSpotToFunding(String apiKey, String apiSecret, String asset, BigDecimal amount) {
        transfer(apiKey, apiSecret, "MAIN_FUNDING", asset, amount);
    }

    private void transfer(String apiKey, String apiSecret, String type, String asset, BigDecimal amount) {
        long ts = System.currentTimeMillis();
        String query = "type=" + type
                + "&asset=" + asset.toUpperCase()
                + "&amount=" + amount.toPlainString()
                + "&timestamp=" + ts
                + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/sapi/v1/asset/transfer?" + query + "&signature=" + signature;
        JsonNode resp = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "Binance transfer failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        if (resp == null || resp.get("tranId") == null) {
            throw new ExchangeException("Unexpected response from Binance transfer API (" + type + ")");
        }
    }

    private OrderStatusResult pollOrderStatus(String apiKey, String apiSecret, String symbol, long orderId) {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            OrderStatusResult status = getOrderStatus(apiKey, apiSecret, symbol, orderId);
            if ("FILLED".equalsIgnoreCase(status.status)
                    || "CANCELED".equalsIgnoreCase(status.status)
                    || "REJECTED".equalsIgnoreCase(status.status)
                    || "EXPIRED".equalsIgnoreCase(status.status)) {
                return status;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ExchangeException("Order polling interrupted", e);
            }
        }
        return getOrderStatus(apiKey, apiSecret, symbol, orderId);
    }

    private OrderStatusResult getOrderStatus(String apiKey, String apiSecret, String symbol, long orderId) {
        long ts = System.currentTimeMillis();
        String query = "symbol=" + symbol + "&orderId=" + orderId + "&timestamp=" + ts + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/api/v3/order?" + query + "&signature=" + signature;
        JsonNode resp = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();
        if (resp == null || resp.get("status") == null) {
            throw new ExchangeException("Unexpected response from Binance order status API");
        }
        String status = resp.get("status").asText();
        BigDecimal executedQty = toDecimal(resp.get("executedQty"));
        return new OrderStatusResult(status, executedQty);
    }

    private record OrderStatusResult(String status, BigDecimal executedQty) {
    }

    private BigDecimal getSpotFreeBalance(String apiKey, String apiSecret, String asset) {
        long ts = System.currentTimeMillis();
        String query = "timestamp=" + ts + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/api/v3/account?" + query + "&signature=" + signature;
        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();
        if (response == null || response.get("balances") == null || !response.get("balances").isArray()) {
            throw new ExchangeException("Unexpected response from Binance spot account API");
        }
        for (JsonNode balance : response.get("balances")) {
            String balanceAsset = balance.hasNonNull("asset") ? balance.get("asset").asText() : null;
            if (balanceAsset != null && balanceAsset.equalsIgnoreCase(asset)) {
                return toDecimal(balance.get("free"));
            }
        }
        return BigDecimal.ZERO;
    }

    private String resolveSymbol(String base, String quote) {
        String candidate = (base + quote).toUpperCase();
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
            throw new ExchangeException("Failed to sign Binance request", e);
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
}
