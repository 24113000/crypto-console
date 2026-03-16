package com.crypto.console.exchanges.bitget;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.Balance;
import com.crypto.console.common.model.BuyInfoItem;
import com.crypto.console.common.model.BuyInfoResult;
import com.crypto.console.common.model.ExchangeCapabilities;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.model.ExchangeTime;
import com.crypto.console.common.model.OrderBook;
import com.crypto.console.common.model.OrderBookEntry;
import com.crypto.console.common.model.OrderResult;
import com.crypto.console.common.model.WithdrawResult;
import com.crypto.console.common.model.WithdrawalFees;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.util.LogSanitizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Slf4j
public class BitgetClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public BitgetClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("bitget", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        JsonNode data = requireOk(signedGet("/api/v2/spot/account/assets", Map.of("coin", asset.toUpperCase())), "account assets");
        if (data != null && data.isArray()) {
            for (JsonNode row : data) {
                String coin = textOf(row, "coin");
                if (asset.equalsIgnoreCase(coin)) {
                    return new Balance(StringUtils.upperCase(coin), dec(row.get("available")), dec(row.get("frozen")));
                }
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /api/v2/spot/public/coins and parse chains[].withdrawFee");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolInfo s = resolveSymbol(base, quote);
        int limit = depth <= 0 ? 150 : Math.min(depth, 150);
        JsonNode data = requireOk(publicGet("/api/v2/spot/market/orderbook", Map.of("symbol", s.symbol, "type", "step0", "limit", String.valueOf(limit))), "orderbook");
        return new OrderBook(s.symbol, parseSide(data.get("bids")), parseSide(data.get("asks")));
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        JsonNode data = requireOk(publicGet("/api/v2/spot/market/orderbook", Map.of("symbol", s.symbol, "type", "step0", "limit", "150")), "orderbook");
        return impact(s.symbol, quoteAmount, data.get("asks"), true);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        JsonNode data = requireOk(publicGet("/api/v2/spot/market/orderbook", Map.of("symbol", s.symbol, "type", "step0", "limit", "150")), "orderbook");
        return impact(s.symbol, quoteAmount, data.get("bids"), false);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        if (s.minTradeUsdt.signum() > 0 && quoteAmount.compareTo(s.minTradeUsdt) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min notional " + s.minTradeUsdt + " for " + s.symbol);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("symbol", s.symbol);
        body.put("side", "buy");
        body.put("orderType", "market");
        body.put("size", quoteAmount.toPlainString());
        body.put("clientOid", "cc-" + UUID.randomUUID());
        JsonNode data = requireOk(signedPost("/api/v2/spot/trade/place-order", body), "place order");
        String orderId = textOf(data, "orderId", "ordId", "clientOid");
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from Bitget place-order response");
        }
        return new OrderResult(orderId, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        BigDecimal qty = baseAmount.setScale(Math.max(0, s.quantityPrecision), RoundingMode.DOWN).stripTrailingZeros();
        if (s.minTradeAmount.signum() > 0 && qty.compareTo(s.minTradeAmount) < 0) {
            throw new ExchangeException("Sell quantity " + qty + " below min quantity " + s.minTradeAmount + " for " + s.symbol);
        }
        if (s.minTradeUsdt.signum() > 0) {
            BigDecimal notional = qty.multiply(priceOf(s.symbol));
            if (notional.compareTo(s.minTradeUsdt) < 0) {
                throw new ExchangeException("Order value " + notional + " below min notional " + s.minTradeUsdt + " for " + s.symbol);
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("symbol", s.symbol);
        body.put("side", "sell");
        body.put("orderType", "market");
        body.put("size", qty.toPlainString());
        body.put("clientOid", "cc-" + UUID.randomUUID());
        JsonNode data = requireOk(signedPost("/api/v2/spot/trade/place-order", body), "place order");
        String orderId = textOf(data, "orderId", "ordId", "clientOid");
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from Bitget place-order response");
        }
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
        CoinInfo c = resolveCoin(asset);
        ChainInfo ch = selectChain(c, network, true);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("coin", c.coin);
        body.put("transferType", "on_chain");
        body.put("address", address);
        body.put("chain", ch.chain);
        body.put("size", amount.toPlainString());
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("tag", memoOrNull);
        }
        body.put("clientOid", "cc-" + UUID.randomUUID());
        JsonNode data = requireOk(signedPost("/api/v2/spot/wallet/withdrawal", body), "withdrawal");
        String withdrawId = textOf(data, "orderId", "clientOid");
        if (StringUtils.isBlank(withdrawId)) {
            throw new ExchangeException("Missing withdrawal id from Bitget withdrawal response");
        }
        return new WithdrawResult(withdrawId, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode data = requireOk(publicGet("/api/v2/public/time", Map.of()), "server time");
        long server = longValue(data, "serverTime");
        if (server <= 0) {
            server = System.currentTimeMillis();
        }
        return new ExchangeTime(server, server - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        CoinInfo c = resolveCoin(asset);
        Set<String> out = new LinkedHashSet<>();
        for (ChainInfo ch : c.chains) {
            if (ch.rechargeable) {
                out.add(StringUtils.upperCase(ch.chain));
            }
        }
        if (out.isEmpty()) {
            throw new ExchangeException("Bitget does not support deposits for asset: " + asset.toUpperCase());
        }
        return out;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        CoinInfo c = resolveCoin(asset);
        ChainInfo ch = selectChain(c, network, false);
        JsonNode data = requireOk(signedGet("/api/v2/spot/wallet/deposit-address", Map.of("coin", c.coin, "chain", ch.chain)), "deposit address");
        String address = textOf(data, "address");
        if (StringUtils.isBlank(address)) {
            throw new ExchangeException("No deposit address returned for " + asset.toUpperCase() + " " + network);
        }
        return address;
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, true, true, true, true, true);
    }

    @Override
    public String normalizeDepositNetwork(String network) {
        if (StringUtils.isBlank(network)) {
            return null;
        }
        String value = network;
        int open = value.indexOf('(');
        int close = value.indexOf(')');
        if (open >= 0 && close > open) {
            value = value.substring(open + 1, close);
        }
        String cleaned = value.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        return switch (cleaned) {
            case "ETH", "ERC20", "ETHEREUM" -> "ERC20";
            case "TRX", "TRC20", "TRON" -> "TRC20";
            case "BEP20", "BSC", "BNBSMARTCHAIN" -> "BEP20";
            case "ARBITRUM", "ARBITRUMONE", "ARB" -> "ARBITRUMONE";
            case "POLYGON", "MATIC" -> "POLYGON";
            case "AVAXC", "AVALANCHECCHAIN" -> "AVAXC";
            case "SOL", "SOLANA" -> "SOL";
            default -> cleaned;
        };
    }

    private SymbolInfo resolveSymbol(String base, String quote) {
        JsonNode data = requireOk(publicGet("/api/v2/spot/public/symbols", Map.of()), "symbols");
        if (data == null || !data.isArray()) {
            throw new ExchangeException("Unexpected response from Bitget symbols API");
        }
        for (JsonNode item : data) {
            String status = textOf(item, "status");
            if (StringUtils.isNotBlank(status) && !"online".equalsIgnoreCase(status)) {
                continue;
            }
            String b = textOf(item, "baseCoin");
            String q = textOf(item, "quoteCoin");
            if (!base.equalsIgnoreCase(b) || !quote.equalsIgnoreCase(q)) {
                continue;
            }
            String symbol = textOf(item, "symbol");
            int qtyPrecision = intValue(item, "quantityPrecision", 8);
            BigDecimal minTradeAmount = dec(item.get("minTradeAmount"));
            BigDecimal minTradeUsdt = dec(item.get("minTradeUSDT"));
            return new SymbolInfo(symbol, qtyPrecision, minTradeAmount, minTradeUsdt);
        }
        throw new ExchangeException("Invalid symbol: " + base.toUpperCase() + "/" + quote.toUpperCase());
    }

    private BigDecimal priceOf(String symbol) {
        JsonNode data = requireOk(publicGet("/api/v2/spot/market/tickers", Map.of("symbol", symbol)), "tickers");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new ExchangeException("Unable to get ticker for " + symbol);
        }
        JsonNode t = data.get(0);
        BigDecimal ask = dec(t.get("askPr"));
        if (ask.signum() > 0) {
            return ask;
        }
        BigDecimal last = dec(t.get("lastPr"));
        if (last.signum() > 0) {
            return last;
        }
        throw new ExchangeException("Unable to determine price for " + symbol);
    }

    private CoinInfo resolveCoin(String asset) {
        JsonNode data = requireOk(publicGet("/api/v2/spot/public/coins", Map.of("coin", asset.toUpperCase())), "coin info");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new ExchangeException("Bitget does not support asset: " + asset.toUpperCase());
        }
        JsonNode coinNode = data.get(0);
        String coin = StringUtils.upperCase(textOf(coinNode, "coin"));
        if (StringUtils.isBlank(coin)) {
            throw new ExchangeException("Bitget does not support asset: " + asset.toUpperCase());
        }
        List<ChainInfo> chains = new ArrayList<>();
        JsonNode chainArr = coinNode.get("chains");
        if (chainArr != null && chainArr.isArray()) {
            for (JsonNode chainNode : chainArr) {
                String chain = textOf(chainNode, "chain");
                if (StringUtils.isBlank(chain)) {
                    continue;
                }
                boolean rechargeable = boolWithDefault(chainNode, true, "rechargeable");
                boolean withdrawable = boolWithDefault(chainNode, true, "withdrawable");
                chains.add(new ChainInfo(chain, rechargeable, withdrawable));
            }
        }
        return new CoinInfo(coin, chains);
    }

    private ChainInfo selectChain(CoinInfo coin, String requested, boolean withdraw) {
        List<ChainInfo> supported = new ArrayList<>();
        for (ChainInfo ch : coin.chains) {
            if (withdraw ? ch.withdrawable : ch.rechargeable) {
                supported.add(ch);
            }
        }
        if (supported.isEmpty()) {
            throw new ExchangeException("Bitget does not support " + (withdraw ? "withdrawal" : "deposit") + " for asset: " + coin.coin);
        }

        String req = normalizeDepositNetwork(requested);
        if (StringUtils.isBlank(req)) {
            if (supported.size() == 1) {
                return supported.get(0);
            }
            StringBuilder available = new StringBuilder();
            for (ChainInfo ch : supported) {
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(ch.chain);
            }
            throw new ExchangeException("Network is required for " + coin.coin + " (available: " + available + ")");
        }
        for (ChainInfo ch : supported) {
            String normalizedChain = normalizeDepositNetwork(ch.chain);
            if (req.equals(normalizedChain)) {
                return ch;
            }
            if (StringUtils.isNotBlank(normalizedChain) && (normalizedChain.contains(req) || req.contains(normalizedChain))) {
                return ch;
            }
        }
        throw new ExchangeException("Bitget does not support network " + requested + " for asset " + coin.coin);
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = buildUri(path, params);
        LOG.info("bitget GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class,
                        ex -> Mono.error(new ExchangeException("Bitget request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "public GET");
    }

    private JsonNode signedGet(String path, Map<String, String> params) {
        TreeMap<String, String> sorted = new TreeMap<>();
        if (params != null) {
            sorted.putAll(params);
        }
        String query = toQuery(sorted);
        String uri = query.isEmpty() ? path : path + "?" + query;
        String ts = String.valueOf(System.currentTimeMillis());
        String sign = sign(ts, "GET", path, query, "");
        LOG.info("bitget GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("ACCESS-KEY", apiKey())
                .header("ACCESS-SIGN", sign)
                .header("ACCESS-TIMESTAMP", ts)
                .header("ACCESS-PASSPHRASE", passphrase())
                .header("locale", "en-US")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class,
                        ex -> Mono.error(new ExchangeException("Bitget request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "signed GET");
    }

    private JsonNode signedPost(String path, Map<String, Object> bodyParams) {
        String body = toJson(bodyParams);
        String ts = String.valueOf(System.currentTimeMillis());
        String sign = sign(ts, "POST", path, "", body);
        LOG.info("bitget POST {}", LogSanitizer.sanitize(path + "?" + body));
        String response = webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header("ACCESS-KEY", apiKey())
                .header("ACCESS-SIGN", sign)
                .header("ACCESS-TIMESTAMP", ts)
                .header("ACCESS-PASSPHRASE", passphrase())
                .header("locale", "en-US")
                .bodyValue(body)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class,
                        ex -> Mono.error(new ExchangeException("Bitget request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(response, "signed POST");
    }

    private String buildUri(String path, Map<String, String> params) {
        UriComponentsBuilder b = UriComponentsBuilder.fromPath(path);
        if (params != null) {
            for (Map.Entry<String, String> e : params.entrySet()) {
                if (e.getValue() != null) {
                    b.queryParam(e.getKey(), e.getValue());
                }
            }
        }
        return b.build(true).toUriString();
    }

    private String sign(String ts, String method, String path, String query, String body) {
        String preHash;
        if (StringUtils.isBlank(query)) {
            preHash = ts + method.toUpperCase() + path + StringUtils.defaultString(body);
        } else {
            preHash = ts + method.toUpperCase() + path + "?" + query + StringUtils.defaultString(body);
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(preHash.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(raw);
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign Bitget request", e);
        }
    }

    private JsonNode requireOk(JsonNode root, String context) {
        if (root == null || root.isNull()) {
            throw new ExchangeException("Unexpected response from Bitget " + context + " API");
        }
        String code = textOf(root, "code");
        if (StringUtils.isNotBlank(code) && !"00000".equals(code)) {
            String msg = textOf(root, "msg", "message");
            throw new ExchangeException("Bitget " + context + " failed: code=" + code + " " + StringUtils.defaultString(msg));
        }
        if (!root.has("data")) {
            return root;
        }
        return root.get("data");
    }

    private BuyInfoResult impact(String symbol, BigDecimal quoteAmount, JsonNode levels, boolean buy) {
        if (levels == null || !levels.isArray()) {
            throw new ExchangeException("Unexpected response from Bitget orderbook API");
        }
        BigDecimal remaining = quoteAmount;
        BigDecimal spentOrReceived = BigDecimal.ZERO;
        BigDecimal baseAmount = BigDecimal.ZERO;
        List<BuyInfoItem> affected = new ArrayList<>();
        for (JsonNode level : levels) {
            if (level == null || !level.isArray() || level.size() < 2) {
                continue;
            }
            BigDecimal price = dec(level.get(0));
            BigDecimal qty = dec(level.get(1));
            if (price.signum() <= 0 || qty.signum() <= 0) {
                continue;
            }
            BigDecimal value = price.multiply(qty);
            if (remaining.compareTo(value) >= 0) {
                spentOrReceived = spentOrReceived.add(value);
                baseAmount = baseAmount.add(qty);
                remaining = remaining.subtract(value);
                affected.add(new BuyInfoItem(price, qty, value));
            } else {
                BigDecimal partialBase = remaining.divide(price, 18, RoundingMode.DOWN);
                if (partialBase.signum() > 0) {
                    BigDecimal partialValue = partialBase.multiply(price);
                    spentOrReceived = spentOrReceived.add(partialValue);
                    baseAmount = baseAmount.add(partialBase);
                    affected.add(new BuyInfoItem(price, partialBase, partialValue));
                }
                remaining = BigDecimal.ZERO;
                break;
            }
        }
        if (baseAmount.signum() <= 0) {
            throw new ExchangeException("No " + (buy ? "ask" : "bid") + " liquidity available for " + symbol);
        }
        BigDecimal avg = spentOrReceived.divide(baseAmount, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol, quoteAmount, spentOrReceived, baseAmount, avg, List.copyOf(affected));
    }

    private List<OrderBookEntry> parseSide(JsonNode side) {
        List<OrderBookEntry> out = new ArrayList<>();
        if (side == null || !side.isArray()) {
            return out;
        }
        for (JsonNode level : side) {
            if (level == null || !level.isArray() || level.size() < 2) {
                continue;
            }
            BigDecimal price = dec(level.get(0));
            BigDecimal qty = dec(level.get(1));
            if (price.signum() > 0 && qty.signum() > 0) {
                out.add(new OrderBookEntry(price, qty));
            }
        }
        return out;
    }

    private JsonNode readJson(String body, String context) {
        try {
            return MAPPER.readTree(StringUtils.defaultIfBlank(body, "{}"));
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse Bitget " + context + " response", e);
        }
    }

    private String toQuery(Map<String, String> params) {
        UriComponentsBuilder b = UriComponentsBuilder.newInstance();
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (e.getValue() != null) {
                b.queryParam(e.getKey(), e.getValue());
            }
        }
        String q = b.build(true).getQuery();
        return q == null ? "" : q;
    }

    private String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize Bitget request body", e);
        }
    }

    private boolean boolWithDefault(JsonNode node, boolean defaultValue, String key) {
        if (node == null || !node.has(key)) {
            return defaultValue;
        }
        JsonNode v = node.get(key);
        if (v.isBoolean()) {
            return v.asBoolean(defaultValue);
        }
        String t = StringUtils.trimToEmpty(v.asText()).toLowerCase();
        if ("true".equals(t) || "1".equals(t) || "yes".equals(t)) {
            return true;
        }
        if ("false".equals(t) || "0".equals(t) || "no".equals(t)) {
            return false;
        }
        return defaultValue;
    }

    private String textOf(JsonNode node, String... keys) {
        if (node == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            if (node.hasNonNull(key)) {
                String value = node.get(key).asText();
                if (StringUtils.isNotBlank(value)) {
                    return value;
                }
            }
        }
        return null;
    }

    private int intValue(JsonNode node, String key, int fallback) {
        if (node == null || !node.has(key)) {
            return fallback;
        }
        try {
            return Integer.parseInt(node.get(key).asText());
        } catch (Exception e) {
            return fallback;
        }
    }

    private long longValue(JsonNode node, String key) {
        if (node == null || !node.has(key)) {
            return 0L;
        }
        try {
            return Long.parseLong(node.get(key).asText());
        } catch (Exception e) {
            return 0L;
        }
    }

    private BigDecimal dec(JsonNode node) {
        if (node == null || node.isNull()) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(node.asText("0"));
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private String apiKey() {
        String key = secrets == null ? null : secrets.getApiKey();
        if (StringUtils.isBlank(key)) {
            throw new ExchangeException("Missing API credentials for bitget");
        }
        return key;
    }

    private String secret() {
        String secret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(secret)) {
            throw new ExchangeException("Missing API credentials for bitget");
        }
        return secret;
    }

    private String passphrase() {
        String passphrase = secrets == null ? null : secrets.getApiPassphrase();
        if (StringUtils.isBlank(passphrase)) {
            throw new ExchangeException("Missing API passphrase for bitget");
        }
        return passphrase;
    }

    private record SymbolInfo(String symbol, int quantityPrecision, BigDecimal minTradeAmount, BigDecimal minTradeUsdt) {
    }

    private record ChainInfo(String chain, boolean rechargeable, boolean withdrawable) {
    }

    private record CoinInfo(String coin, List<ChainInfo> chains) {
    }
}
