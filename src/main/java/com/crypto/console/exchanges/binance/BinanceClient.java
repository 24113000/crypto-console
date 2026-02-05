package com.crypto.console.exchanges.binance;

import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.util.LogSanitizer;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

@Slf4j
public class BinanceClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
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
        String query = "timestamp=" + timestamp + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/api/v3/account?" + query + "&signature=" + signature;

        LOG.info("binance GET {}", LogSanitizer.sanitize(uri));
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
        throw notImplemented("GET /sapi/v1/capital/config/getall (signed)");
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
        LOG.info("binance GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        if (response == null || response.get("asks") == null || !response.get("asks").isArray()) {
            throw new ExchangeException("Unexpected response from Binance depth API");
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
                    boughtBase = boughtBase.add(partialQty);
                    BigDecimal partialCost = partialQty.multiply(price);
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
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
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
        LOG.info("binance GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        if (response == null || response.get("bids") == null || !response.get("bids").isArray()) {
            throw new ExchangeException("Unexpected response from Binance depth API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal receivedQuote = BigDecimal.ZERO;
        BigDecimal soldBase = BigDecimal.ZERO;
        List<BuyInfoItem> affectedItems = new ArrayList<>();

        for (JsonNode bid : response.get("bids")) {
            if (bid == null || !bid.isArray() || bid.size() < 2) {
                continue;
            }
            BigDecimal price = toDecimal(bid.get(0));
            BigDecimal quantity = toDecimal(bid.get(1));
            if (price.signum() <= 0 || quantity.signum() <= 0) {
                continue;
            }
            BigDecimal levelQuoteValue = price.multiply(quantity);
            if (remainingQuote.compareTo(levelQuoteValue) >= 0) {
                soldBase = soldBase.add(quantity);
                receivedQuote = receivedQuote.add(levelQuoteValue);
                affectedItems.add(new BuyInfoItem(price, quantity, levelQuoteValue));
                remainingQuote = remainingQuote.subtract(levelQuoteValue);
            } else {
                BigDecimal partialQty = remainingQuote.divide(price, 18, RoundingMode.DOWN);
                if (partialQty.signum() > 0) {
                    BigDecimal partialValue = partialQty.multiply(price);
                    soldBase = soldBase.add(partialQty);
                    receivedQuote = receivedQuote.add(partialValue);
                    affectedItems.add(new BuyInfoItem(price, partialQty, partialValue));
                }
                remainingQuote = BigDecimal.ZERO;
                break;
            }
            if (remainingQuote.signum() == 0) {
                break;
            }
        }

        if (soldBase.signum() <= 0) {
            throw new ExchangeException("No bid liquidity available for " + symbol);
        }

        BigDecimal averagePrice = receivedQuote.divide(soldBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol, quoteAmount, receivedQuote, soldBase, averagePrice, List.copyOf(affectedItems));
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        return executeMarketOrder(
                OrderSide.BUY,
                base,
                quote,
                quoteAmount,
                true
        );
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        return executeMarketOrder(
                OrderSide.SELL,
                base,
                quote,
                baseAmount,
                false
        );
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
            throw new ExchangeException("Missing API credentials for binance");
        }

        long ts = System.currentTimeMillis();
        StringBuilder query = new StringBuilder();
        query.append("coin=").append(asset.toUpperCase());
        query.append("&address=").append(address);
        query.append("&amount=").append(amount.toPlainString());
        if (StringUtils.isNotBlank(network)) {
            query.append("&network=").append(normalizeDepositNetwork(network));
        }
        //query.append("&walletType=1");//ommit we want to work wit SPOT
        if (StringUtils.isNotBlank(memoOrNull)) {
            query.append("&addressTag=").append(memoOrNull);
        }
        query.append("&timestamp=").append(ts);
        query.append("&recvWindow=5000");

        String signature = sign(query.toString(), apiSecret);
        String uri = "/sapi/v1/capital/withdraw/apply?" + query + "&signature=" + signature;

        LOG.info("binance POST {}", LogSanitizer.sanitize(uri));
        JsonNode response = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "Binance withdraw failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();

        if (response == null) {
            throw new ExchangeException("Unexpected response from Binance withdraw API");
        }
        String id = response.hasNonNull("id") ? response.get("id").asText() : null;
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from Binance withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
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

        LOG.info("binance GET {}", LogSanitizer.sanitize(uri));
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
        String normalizedNetwork = normalizeDepositNetwork(network);
        StringBuilder query = new StringBuilder();
        query.append("coin=").append(asset.toUpperCase());
        if (StringUtils.isNotBlank(normalizedNetwork)) {
            query.append("&network=").append(normalizedNetwork);
        }
        query.append("&timestamp=").append(timestamp);
        query.append("&recvWindow=5000");

        String signature = sign(query.toString(), apiSecret);
        String uri = "/sapi/v1/capital/deposit/address?" + query + "&signature=" + signature;

        LOG.info("binance GET {}", LogSanitizer.sanitize(uri));
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
        LOG.info("binance POST {}", LogSanitizer.sanitize(uri));
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
        LOG.info("binance GET {}", LogSanitizer.sanitize(uri));
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

    private OrderResult executeMarketOrder(OrderSide side, String base, String quote, BigDecimal amount, boolean isQuoteAmount) {
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for binance");
        }

        // 1) Resolve symbol and place market order.
        String symbol = resolveSymbol(base, quote);
        if (symbol == null) {
            throw new ExchangeException("Invalid symbol: " + (base + quote).toUpperCase() + ". Check base/quote assets.");
        }
        long ts = System.currentTimeMillis();
        StringBuilder orderQuery = new StringBuilder();
        orderQuery.append("symbol=").append(symbol)
                .append("&side=").append(side.name())
                .append("&type=MARKET");
        if (isQuoteAmount) {
            orderQuery.append("&quoteOrderQty=").append(amount.toPlainString());
        } else {
            BigDecimal spotFree = getSpotFreeBalance(apiKey, apiSecret, base);
            BigDecimal rawQty = spotFree.min(amount);
            BigDecimal qty = applyLotSize(symbol, rawQty);
            if (qty.signum() <= 0) {
                throw new ExchangeException("Sell quantity below minimum lot size for " + symbol);
            }
            BigDecimal minNotional = getMinNotional(symbol);
            if (minNotional != null && minNotional.signum() > 0) {
                BigDecimal price = getAvgPrice(symbol);
                if (price != null && price.signum() > 0) {
                    BigDecimal notional = qty.multiply(price);
                    if (notional.compareTo(minNotional) < 0) {
                        throw new ExchangeException("Order value " + notional + " below min notional " + minNotional + " for " + symbol);
                    }
                }
            }
            orderQuery.append("&quantity=").append(qty.toPlainString());
        }
        orderQuery.append("&newOrderRespType=RESULT")
                .append("&timestamp=").append(ts)
                .append("&recvWindow=5000");

        String orderSignature = sign(orderQuery.toString(), apiSecret);
        String orderUri = "/api/v3/order?" + orderQuery + "&signature=" + orderSignature;

        LOG.info("binance POST {}", LogSanitizer.sanitize(orderUri));
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

        // 2) Wait until filled.
        if (!"FILLED".equalsIgnoreCase(status)) {
            OrderStatusResult finalStatus = pollOrderStatus(apiKey, apiSecret, symbol, orderId);
            status = finalStatus.status;
            executedQty = finalStatus.executedQty;
        }

        if (!"FILLED".equalsIgnoreCase(status)) {
            return new OrderResult(String.valueOf(orderId), status, "Order not filled");
        }

        if (side == OrderSide.BUY) {
            if (executedQty.signum() <= 0) {
                throw new ExchangeException("Order filled but executed quantity is zero");
            }
            return new OrderResult(String.valueOf(orderId), status, "filledQty=" + executedQty);
        }

        return new OrderResult(String.valueOf(orderId), status, "filled");
    }

    private enum OrderSide {
        BUY,
        SELL
    }

    private BigDecimal applyLotSize(String symbol, BigDecimal quantity) {
        LotSize lot = getLotSize(symbol);
        if (lot == null) {
            return quantity;
        }
        BigDecimal minQty = lot.minQty;
        BigDecimal maxQty = lot.maxQty;
        BigDecimal step = lot.stepSize;

        if (maxQty != null && maxQty.signum() > 0 && quantity.compareTo(maxQty) > 0) {
            quantity = maxQty;
        }
        if (minQty != null && minQty.signum() > 0 && quantity.compareTo(minQty) < 0) {
            return BigDecimal.ZERO;
        }
        if (step == null || step.signum() <= 0) {
            return quantity;
        }
        BigDecimal steps = quantity.divide(step, 0, java.math.RoundingMode.DOWN);
        BigDecimal adjusted = step.multiply(steps);
        if (minQty != null && minQty.signum() > 0 && adjusted.compareTo(minQty) < 0) {
            return BigDecimal.ZERO;
        }
        return adjusted.stripTrailingZeros();
    }

    private LotSize getLotSize(String symbol) {
        LOG.info("binance GET {}", LogSanitizer.sanitize("/api/v3/exchangeInfo?symbol=" + symbol));
        JsonNode info = webClient.get()
                .uri("/api/v3/exchangeInfo?symbol=" + symbol)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> reactor.core.publisher.Mono.empty())
                .block();
        if (info == null || !info.has("symbols") || !info.get("symbols").isArray()) {
            return null;
        }
        JsonNode first = info.get("symbols").size() > 0 ? info.get("symbols").get(0) : null;
        if (first == null || !first.has("filters") || !first.get("filters").isArray()) {
            return null;
        }
        for (JsonNode filter : first.get("filters")) {
            String type = filter.hasNonNull("filterType") ? filter.get("filterType").asText() : null;
            if ("LOT_SIZE".equalsIgnoreCase(type)) {
                BigDecimal minQty = toDecimal(filter.get("minQty"));
                BigDecimal maxQty = toDecimal(filter.get("maxQty"));
                BigDecimal stepSize = toDecimal(filter.get("stepSize"));
                return new LotSize(minQty, maxQty, stepSize);
            }
        }
        return null;
    }

    private BigDecimal getMinNotional(String symbol) {
        LOG.info("binance GET {}", LogSanitizer.sanitize("/api/v3/exchangeInfo?symbol=" + symbol));
        JsonNode info = webClient.get()
                .uri("/api/v3/exchangeInfo?symbol=" + symbol)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> reactor.core.publisher.Mono.empty())
                .block();
        if (info == null || !info.has("symbols") || !info.get("symbols").isArray()) {
            return null;
        }
        JsonNode first = info.get("symbols").size() > 0 ? info.get("symbols").get(0) : null;
        if (first == null || !first.has("filters") || !first.get("filters").isArray()) {
            return null;
        }
        for (JsonNode filter : first.get("filters")) {
            String type = filter.hasNonNull("filterType") ? filter.get("filterType").asText() : null;
            if ("NOTIONAL".equalsIgnoreCase(type)) {
                return toDecimal(filter.get("minNotional"));
            }
            if ("MIN_NOTIONAL".equalsIgnoreCase(type)) {
                return toDecimal(filter.get("minNotional"));
            }
        }
        return null;
    }

    private BigDecimal getAvgPrice(String symbol) {
        LOG.info("binance GET {}", LogSanitizer.sanitize("/api/v3/avgPrice?symbol=" + symbol));
        JsonNode resp = webClient.get()
                .uri("/api/v3/avgPrice?symbol=" + symbol)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> reactor.core.publisher.Mono.empty())
                .block();
        if (resp == null) {
            return null;
        }
        return toDecimal(resp.get("price"));
    }

    private record LotSize(BigDecimal minQty, BigDecimal maxQty, BigDecimal stepSize) {
    }

    private BigDecimal getSpotFreeBalance(String apiKey, String apiSecret, String asset) {
        long ts = System.currentTimeMillis();
        String query = "timestamp=" + ts + "&recvWindow=5000";
        String signature = sign(query, apiSecret);
        String uri = "/api/v3/account?" + query + "&signature=" + signature;
        LOG.info("binance GET {}", LogSanitizer.sanitize(uri));
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
        LOG.info("binance GET {}", LogSanitizer.sanitize("/api/v3/exchangeInfo?symbol=" + candidate));
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

        LOG.info("binance GET /api/v3/exchangeInfo");
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
            case "ETHEREUM", "ERC20", "ETH" -> "ETH";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL", "SOLANASOL" -> "SOL";
            case "TRON", "TRC20", "TRX", "TRONTRC20" -> "TRX";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "KAIA" -> "KAIA";
            case "CELO" -> "CELO";
            case "HECO" -> "HECO";
            case "PLASMA" -> "PLASMA";
            default -> cleaned;
        };
    }
}
