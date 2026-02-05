package com.crypto.console.exchanges.xt;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.util.LogSanitizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.*;

@Slf4j
public class XtClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ALGORITHM = "HmacSHA256";

    public XtClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("xt", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for xt");
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("currencies", asset.toLowerCase());
        JsonNode response = signedGet("/v4/balances", params, apiKey, apiSecret);
        JsonNode result = requireOk(response, "balances").path("result");
        JsonNode assets = result.path("assets");
        if (assets.isArray()) {
            for (JsonNode item : assets) {
                String currency = item.hasNonNull("currency") ? item.get("currency").asText() : null;
                if (currency != null && currency.equalsIgnoreCase(asset)) {
                    BigDecimal free = toDecimal(item.get("availableAmount"));
                    BigDecimal locked = toDecimal(item.get("frozenAmount"));
                    return new Balance(currency.toUpperCase(), free, locked);
                }
            }
        }

        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /v4/public/wallet/support/currency (public)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /v4/public/depth (public)");
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String symbol = (base + "_" + quote).toLowerCase();
        SymbolInfo symbolInfo = getSymbolInfo(symbol);
        if (symbolInfo == null) {
            throw new ExchangeException("Invalid symbol: " + symbol);
        }
        String resolvedSymbol = StringUtils.isNotBlank(symbolInfo.symbol) ? symbolInfo.symbol : symbol;
        if (!symbolInfo.tradingEnabled) {
            throw new ExchangeException("XT symbol not tradable: " + resolvedSymbol);
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", resolvedSymbol);
        params.put("limit", "200");
        String uri = "/v4/public/depth?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode result = requireOk(response, "order book").path("result");
        JsonNode asks = result.get("asks");
        if (asks == null || !asks.isArray()) {
            throw new ExchangeException("Unexpected response from XT order book API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal spentQuote = BigDecimal.ZERO;
        BigDecimal boughtBase = BigDecimal.ZERO;
        List<BuyInfoItem> affectedItems = new ArrayList<>();

        for (JsonNode ask : asks) {
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
            throw new ExchangeException("No ask liquidity available for " + resolvedSymbol);
        }

        BigDecimal averagePrice = spentQuote.divide(boughtBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(resolvedSymbol.toUpperCase(), quoteAmount, spentQuote, boughtBase, averagePrice, List.copyOf(affectedItems));
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        return submitMarketOrder(OrderSide.BUY, base, quote, quoteAmount, true);
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        return submitMarketOrder(OrderSide.SELL, base, quote, baseAmount, false);
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
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for xt");
        }

        SupportedCurrency currency = resolveCurrency(asset);
        String chain = resolveChain(currency, network, true);
        if (StringUtils.isBlank(chain)) {
            throw new ExchangeException("XT does not support withdrawals for asset: " + asset.toUpperCase());
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("currency", currency.currency);
        body.put("chain", chain);
        body.put("amount", amount.toPlainString());
        body.put("address", address);
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("memo", memoOrNull);
        }

        JsonNode response = signedPost("/v4/withdraw", body, apiKey, apiSecret);
        JsonNode result = requireOk(response, "withdraw").path("result");
        String id = null;
        if (result.isTextual() || result.isNumber()) {
            id = result.asText();
        } else if (result.hasNonNull("id")) {
            id = result.get("id").asText();
        } else if (response != null && response.hasNonNull("id")) {
            id = response.get("id").asText();
        }
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from XT withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode response = publicGet("/v4/public/time");
        JsonNode result = requireOk(response, "server time").path("result");
        long serverTime = result.hasNonNull("serverTime") ? result.get("serverTime").asLong() : System.currentTimeMillis();
        long offset = serverTime - System.currentTimeMillis();
        return new ExchangeTime(serverTime, offset);
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        SupportedCurrency currency = resolveCurrency(asset);
        Set<String> networks = new HashSet<>();
        for (SupportChain chain : currency.supportChains) {
            if (chain.depositEnabled && StringUtils.isNotBlank(chain.chain)) {
                networks.add(chain.chain.trim());
            }
        }
        if (networks.isEmpty()) {
            throw new ExchangeException("XT does not support deposits for asset: " + asset.toUpperCase());
        }
        return networks;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for xt");
        }

        SupportedCurrency currency = resolveCurrency(asset);
        String chain = resolveChain(currency, network, false);
        if (StringUtils.isBlank(chain)) {
            throw new ExchangeException("XT does not support deposits for asset: " + asset.toUpperCase());
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("currency", currency.currency);
        params.put("chain", chain);
        JsonNode response = signedGet("/v4/deposit/address", params, apiKey, apiSecret);
        JsonNode result = requireOk(response, "deposit address").path("result");
        JsonNode addressNode = result.get("address");
        return addressNode == null ? null : addressNode.asText();
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, false, true, true, true, true);
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
            case "ETHEREUM", "ERC20", "ETH" -> "ETHEREUM";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL", "SOLANASOL" -> "SOL";
            case "TRON", "TRC20", "TRX" -> "TRON";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "XTSMARTCHAIN", "XSC", "XTCHAIN", "XT" -> "XTSMARTCHAIN";
            default -> cleaned;
        };
    }

    private OrderResult submitMarketOrder(OrderSide side, String base, String quote, BigDecimal amount, boolean isQuoteAmount) {
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for xt");
        }

        String symbol = (base + "_" + quote).toLowerCase();
        SymbolInfo symbolInfo = getSymbolInfo(symbol);
        if (symbolInfo == null) {
            throw new ExchangeException("Invalid symbol: " + symbol);
        }
        String resolvedSymbol = StringUtils.isNotBlank(symbolInfo.symbol) ? symbolInfo.symbol : symbol;
        if (!symbolInfo.tradingEnabled) {
            throw new ExchangeException("XT symbol not tradable: " + resolvedSymbol);
        }
        if (!symbolInfo.openapiEnabled) {
            throw new ExchangeException("XT symbol does not support API trading: " + resolvedSymbol);
        }
        if (!symbolInfo.orderTypes.contains("MARKET")) {
            throw new ExchangeException("XT symbol does not support MARKET orders: " + resolvedSymbol);
        }

        if (isQuoteAmount) {
            BigDecimal minQuote = symbolInfo.minQuoteQty;
            if (minQuote != null && minQuote.signum() > 0 && amount.compareTo(minQuote) < 0) {
                throw new ExchangeException("Order value " + amount + " below min notional " + minQuote + " for " + symbol);
            }
        } else {
            BigDecimal qty = applyLotSize(symbolInfo, amount);
            if (qty.signum() <= 0) {
                throw new ExchangeException("Sell quantity below minimum lot size for " + symbol);
            }
            BigDecimal minQuote = symbolInfo.minQuoteQty;
            if (minQuote != null && minQuote.signum() > 0) {
                BigDecimal price = getLatestPrice(resolvedSymbol);
                if (price != null && price.signum() > 0) {
                    BigDecimal notional = qty.multiply(price);
                    if (notional.compareTo(minQuote) < 0) {
                        throw new ExchangeException("Order value " + notional + " below min notional " + minQuote + " for " + resolvedSymbol);
                    }
                }
            }
            amount = qty;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("symbol", resolvedSymbol);
        body.put("side", side.name());
        body.put("type", "MARKET");
        body.put("timeInForce", "FOK");
        body.put("bizType", "SPOT");
        if (isQuoteAmount) {
            body.put("quoteQty", amount.toPlainString());
        } else {
            body.put("quantity", amount.toPlainString());
        }

        JsonNode response = signedPost("/v4/order", body, apiKey, apiSecret);
        JsonNode result = requireOk(response, "order").path("result");
        String orderId = result.hasNonNull("orderId") ? result.get("orderId").asText() : null;
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from XT order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market order submitted");
    }

    private SymbolInfo getSymbolInfo(String symbol) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        String uri = "/v4/public/symbol?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode result = requireOk(response, "symbol info").path("result");
        JsonNode symbols = result.path("symbols");
        if (!symbols.isArray() || symbols.isEmpty()) {
            return null;
        }
        JsonNode node = symbols.get(0);
        if (node == null) {
            return null;
        }
        boolean tradingEnabled = node.hasNonNull("tradingEnabled") && node.get("tradingEnabled").asBoolean(false);
        boolean openapiEnabled = node.hasNonNull("openapiEnabled") && node.get("openapiEnabled").asBoolean(false);
        Set<String> orderTypes = new HashSet<>();
        JsonNode orderTypesNode = node.get("orderTypes");
        if (orderTypesNode != null && orderTypesNode.isArray()) {
            for (JsonNode item : orderTypesNode) {
                String type = item.asText(null);
                if (StringUtils.isNotBlank(type)) {
                    orderTypes.add(type.toUpperCase());
                }
            }
        }
        BigDecimal minQty = null;
        BigDecimal maxQty = null;
        BigDecimal step = null;
        BigDecimal minQuote = null;
        JsonNode filters = node.get("filters");
        if (filters != null && filters.isArray()) {
            for (JsonNode filter : filters) {
                String type = filter.hasNonNull("filter") ? filter.get("filter").asText() : null;
                if ("QUANTITY".equalsIgnoreCase(type)) {
                    minQty = toDecimal(filter.get("min"));
                    maxQty = toDecimal(filter.get("max"));
                    step = toDecimal(filter.get("tickSize"));
                } else if ("QUOTE_QTY".equalsIgnoreCase(type)) {
                    minQuote = toDecimal(filter.get("min"));
                }
            }
        }
        String canonicalSymbol = node.hasNonNull("symbol") ? node.get("symbol").asText() : symbol;
        return new SymbolInfo(canonicalSymbol, tradingEnabled, openapiEnabled, orderTypes, minQty, maxQty, step, minQuote);
    }

    private BigDecimal getLatestPrice(String symbol) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        String uri = "/v4/public/ticker/price?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode result = requireOk(response, "price").path("result");
        if (result.isArray() && result.size() > 0) {
            JsonNode item = result.get(0);
            return toDecimal(item.get("p"));
        }
        return null;
    }

    private BigDecimal applyLotSize(SymbolInfo info, BigDecimal quantity) {
        if (info == null) {
            return quantity;
        }
        BigDecimal minQty = info.minQty;
        BigDecimal maxQty = info.maxQty;
        BigDecimal step = info.stepSize;
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

    private JsonNode signedGet(String path, Map<String, String> params, String apiKey, String apiSecret) {
        String query = params == null ? "" : buildSortedQueryString(params);
        String uri = query.isEmpty() ? path : path + "?" + query;
        SignedHeaders headers = signRequest("GET", path, query, null, apiKey, apiSecret);
        LOG.info("xt GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header("validate-algorithms", ALGORITHM)
                .header("validate-appkey", apiKey)
                .header("validate-recvwindow", headers.recvWindow)
                .header("validate-timestamp", headers.timestamp)
                .header("validate-signature", headers.signature)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "XT request failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode signedPost(String path, Map<String, Object> body, String apiKey, String apiSecret) {
        String jsonBody = "";
        try {
            if (body != null) {
                jsonBody = MAPPER.writeValueAsString(body);
            }
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize XT request body", e);
        }
        SignedHeaders headers = signRequest("POST", path, "", jsonBody, apiKey, apiSecret);
        LOG.info("xt POST {}", LogSanitizer.sanitize(path));
        return webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header("validate-algorithms", ALGORITHM)
                .header("validate-appkey", apiKey)
                .header("validate-recvwindow", headers.recvWindow)
                .header("validate-timestamp", headers.timestamp)
                .header("validate-signature", headers.signature)
                .bodyValue(jsonBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String bodyText = ex.getResponseBodyAsString();
                    String msg = "XT request failed: HTTP " + ex.getStatusCode().value();
                    if (bodyText != null && !bodyText.isBlank()) {
                        msg = msg + " body=" + bodyText;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode publicGet(String uri) {
        LOG.info("xt GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "XT request failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private SignedHeaders signRequest(String method, String path, String query, String body, String apiKey, String apiSecret) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String recvWindow = "5000";
        String headerPart = "validate-algorithms=" + ALGORITHM
                + "&validate-appkey=" + apiKey
                + "&validate-recvwindow=" + recvWindow
                + "&validate-timestamp=" + timestamp;
        String dataPart = buildDataPart(method, path, query, body);
        String signature = hmacSha256Hex(apiSecret, headerPart + dataPart);
        return new SignedHeaders(timestamp, recvWindow, signature);
    }

    private String buildDataPart(String method, String path, String query, String body) {
        StringBuilder sb = new StringBuilder();
        sb.append("#").append(method.toUpperCase()).append("#").append(path);
        if (StringUtils.isNotBlank(query)) {
            sb.append("#").append(query);
        }
        if (StringUtils.isNotBlank(body)) {
            sb.append("#").append(body);
        }
        return sb.toString();
    }

    private String buildQueryString(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (!sb.isEmpty()) {
                sb.append("&");
            }
            sb.append(encodeQuery(entry.getKey()))
                    .append("=")
                    .append(encodeQuery(entry.getValue()));
        }
        return sb.toString();
    }

    private String buildSortedQueryString(Map<String, String> params) {
        Map<String, String> sorted = new TreeMap<>(params);
        return buildQueryString(sorted);
    }

    private String encodeQuery(String value) {
        try {
            String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
            return encoded.replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode XT query parameter", e);
        }
    }

    private String hmacSha256Hex(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign XT request", e);
        }
    }

    private SupportedCurrency resolveCurrency(String asset) {
        List<SupportedCurrency> currencies = getSupportedCurrencies();
        for (SupportedCurrency item : currencies) {
            if (item.currency != null && item.currency.equalsIgnoreCase(asset)) {
                return item;
            }
        }
        throw new ExchangeException("XT does not support asset: " + asset.toUpperCase());
    }

    private String resolveChain(SupportedCurrency currency, String network, boolean forWithdraw) {
        if (currency == null) {
            return null;
        }
        List<SupportChain> candidates = new ArrayList<>();
        for (SupportChain chain : currency.supportChains) {
            if (forWithdraw && !chain.withdrawEnabled) {
                continue;
            }
            if (!forWithdraw && !chain.depositEnabled) {
                continue;
            }
            candidates.add(chain);
        }
        if (candidates.isEmpty()) {
            return null;
        }
        String normalized = normalizeDepositNetwork(network);
        if (StringUtils.isBlank(normalized)) {
            if (candidates.size() == 1) {
                return normalizeChainParam(candidates.get(0).chain);
            }
            StringBuilder available = new StringBuilder();
            for (SupportChain chain : candidates) {
                if (StringUtils.isBlank(chain.chain)) {
                    continue;
                }
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(chain.chain);
            }
            throw new ExchangeException("Network is required for " + currency.currency + " (available: " + available + ")");
        }
        for (SupportChain chain : candidates) {
            String chainName = chain.chain;
            if (StringUtils.isBlank(chainName)) {
                continue;
            }
            String normalizedChain = normalizeDepositNetwork(chainName);
            if (normalized.equalsIgnoreCase(normalizedChain)) {
                return normalizeChainParam(chainName);
            }
        }
        throw new ExchangeException("XT does not support network " + network + " for asset " + currency.currency.toUpperCase());
    }

    private String normalizeChainParam(String chain) {
        if (StringUtils.isBlank(chain)) {
            return chain;
        }
        int openIdx = chain.indexOf('(');
        int closeIdx = chain.indexOf(')');
        if (openIdx >= 0 && closeIdx > openIdx) {
            String inner = chain.substring(openIdx + 1, closeIdx);
            if (StringUtils.isNotBlank(inner)) {
                return inner;
            }
        }
        return chain;
    }

    private List<SupportedCurrency> getSupportedCurrencies() {
        JsonNode response = publicGet("/v4/public/wallet/support/currency");
        JsonNode result = requireOk(response, "supported currencies").path("result");
        if (!result.isArray()) {
            throw new ExchangeException("Unexpected response from XT supported currencies API");
        }
        List<SupportedCurrency> currencies = new ArrayList<>();
        for (JsonNode item : result) {
            String currency = item.hasNonNull("currency") ? item.get("currency").asText() : null;
            List<SupportChain> chains = new ArrayList<>();
            JsonNode chainList = item.get("supportChains");
            if (chainList != null && chainList.isArray()) {
                for (JsonNode chainNode : chainList) {
                    String chain = chainNode.hasNonNull("chain") ? chainNode.get("chain").asText() : null;
                    boolean depositEnabled = chainNode.hasNonNull("depositEnabled") && chainNode.get("depositEnabled").asBoolean(false);
                    boolean withdrawEnabled = chainNode.hasNonNull("withdrawEnabled") && chainNode.get("withdrawEnabled").asBoolean(false);
                    chains.add(new SupportChain(chain, depositEnabled, withdrawEnabled));
                }
            }
            currencies.add(new SupportedCurrency(currency, chains));
        }
        return currencies;
    }

    private JsonNode requireOk(JsonNode response, String context) {
        if (response == null) {
            throw new ExchangeException("Unexpected response from XT " + context + " API");
        }
        if (response.hasNonNull("rc")) {
            int rc = response.get("rc").asInt(-1);
            if (rc != 0) {
                String msg = response.hasNonNull("mc") ? response.get("mc").asText() : "";
                throw new ExchangeException("XT " + context + " failed: rc=" + rc + " " + msg);
            }
        }
        return response;
    }

    private String getApiKey() {
        if (secrets == null) {
            return null;
        }
        return StringUtils.trimToNull(secrets.getApiKey());
    }

    private String getApiSecret() {
        if (secrets == null) {
            return null;
        }
        return StringUtils.trimToNull(secrets.getApiSecret());
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

    private enum OrderSide {
        BUY,
        SELL
    }

    private record SignedHeaders(String timestamp, String recvWindow, String signature) {
    }

    private record SymbolInfo(String symbol, boolean tradingEnabled, boolean openapiEnabled, Set<String> orderTypes,
                              BigDecimal minQty, BigDecimal maxQty, BigDecimal stepSize, BigDecimal minQuoteQty) {
    }

    private record SupportedCurrency(String currency, List<SupportChain> supportChains) {
    }

    private record SupportChain(String chain, boolean depositEnabled, boolean withdrawEnabled) {
    }
}
