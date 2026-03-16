package com.crypto.console.exchanges.bybit;

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
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
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
public class BybitClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String RECV_WINDOW = "5000";

    public BybitClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("bybit", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        JsonNode result = requireOk(signedGet("/v5/account/wallet-balance", Map.of("accountType", "UNIFIED", "coin", asset.toUpperCase())), "wallet balance");
        JsonNode list = result == null ? null : result.get("list");
        if (list != null && list.isArray() && !list.isEmpty()) {
            JsonNode coins = list.get(0).get("coin");
            if (coins != null && coins.isArray()) {
                for (JsonNode c : coins) {
                    String coin = textOf(c, "coin");
                    if (!asset.equalsIgnoreCase(coin)) {
                        continue;
                    }
                    BigDecimal wallet = dec(c.get("walletBalance"));
                    BigDecimal available = dec(c.get("availableToWithdraw"));
                    BigDecimal free = available.signum() > 0 ? available : wallet;
                    BigDecimal locked = wallet.subtract(free);
                    if (locked.signum() < 0) {
                        locked = BigDecimal.ZERO;
                    }
                    return new Balance(StringUtils.upperCase(coin), free, locked);
                }
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /v5/asset/coin/query-info and parse chains[].withdrawFee");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolMeta s = resolveSymbol(base, quote);
        int limit = depth <= 0 ? 50 : Math.min(depth, 200);
        JsonNode result = requireOk(publicGet("/v5/market/orderbook", Map.of("category", "spot", "symbol", s.symbol, "limit", String.valueOf(limit))), "orderbook");
        return new OrderBook(s.symbol, parseSide(result.get("b")), parseSide(result.get("a")));
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        JsonNode result = requireOk(publicGet("/v5/market/orderbook", Map.of("category", "spot", "symbol", s.symbol, "limit", "200")), "orderbook");
        return impact(s.symbol, quoteAmount, result.get("a"), true);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        JsonNode result = requireOk(publicGet("/v5/market/orderbook", Map.of("category", "spot", "symbol", s.symbol, "limit", "200")), "orderbook");
        return impact(s.symbol, quoteAmount, result.get("b"), false);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        if (s.minNotional.signum() > 0 && quoteAmount.compareTo(s.minNotional) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min notional " + s.minNotional + " for " + s.symbol);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("category", "spot");
        body.put("symbol", s.symbol);
        body.put("side", "Buy");
        body.put("orderType", "Market");
        body.put("marketUnit", "quoteCoin");
        body.put("qty", quoteAmount.toPlainString());
        body.put("orderLinkId", "cc-" + UUID.randomUUID());
        JsonNode result = requireOk(signedPost("/v5/order/create", body), "place order");
        String orderId = textOf(result, "orderId", "orderLinkId");
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from Bybit order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        BigDecimal qty = applyStep(baseAmount, s.qtyStep);
        if (s.minQty.signum() > 0 && qty.compareTo(s.minQty) < 0) {
            throw new ExchangeException("Sell quantity " + qty + " below min quantity " + s.minQty + " for " + s.symbol);
        }
        if (s.minNotional.signum() > 0) {
            BigDecimal price = priceOf(s.symbol);
            if (price.signum() > 0 && qty.multiply(price).compareTo(s.minNotional) < 0) {
                throw new ExchangeException("Order value below min notional " + s.minNotional + " for " + s.symbol);
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("category", "spot");
        body.put("symbol", s.symbol);
        body.put("side", "Sell");
        body.put("orderType", "Market");
        body.put("qty", qty.toPlainString());
        body.put("orderLinkId", "cc-" + UUID.randomUUID());
        JsonNode result = requireOk(signedPost("/v5/order/create", body), "place order");
        String orderId = textOf(result, "orderId", "orderLinkId");
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from Bybit order API");
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
        CoinMeta coin = resolveCoin(asset);
        ChainMeta chain = selectChain(coin, network, true);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("coin", coin.coin);
        body.put("chain", chain.chain);
        body.put("address", address);
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("tag", memoOrNull);
        }
        body.put("amount", amount.toPlainString());
        body.put("timestamp", System.currentTimeMillis());
        body.put("forceChain", 1);
        body.put("accountType", "FUND,UTA");
        body.put("requestId", "cc-" + UUID.randomUUID());
        JsonNode result = requireOk(signedPost("/v5/asset/withdraw/create", body), "withdraw");
        String id = textOf(result, "id");
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from Bybit withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode result = requireOk(publicGet("/v5/market/time", Map.of()), "server time");
        long t = result == null ? 0L : result.path("timeSecond").asLong(0L) * 1000L;
        if (t <= 0L) {
            t = System.currentTimeMillis();
        }
        return new ExchangeTime(t, t - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        CoinMeta c = resolveCoin(asset);
        Set<String> out = new LinkedHashSet<>();
        for (ChainMeta ch : c.chains) {
            if (ch.depositEnabled) {
                out.add(StringUtils.upperCase(ch.chain));
            }
        }
        if (out.isEmpty()) {
            throw new ExchangeException("Bybit does not support deposits for asset: " + asset.toUpperCase());
        }
        return out;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        CoinMeta c = resolveCoin(asset);
        ChainMeta ch = selectChain(c, network, false);
        JsonNode result = requireOk(signedGet("/v5/asset/deposit/query-address", Map.of("coin", c.coin, "chainType", ch.chain)), "deposit address");
        JsonNode chains = result == null ? null : result.get("chains");
        if (chains != null && chains.isArray()) {
            String req = normalizeDepositNetwork(ch.chain);
            for (JsonNode node : chains) {
                String chain = textOf(node, "chain");
                if (StringUtils.isBlank(chain)) {
                    continue;
                }
                String normalized = normalizeDepositNetwork(chain);
                if (!req.equals(normalized) && !(normalized != null && (normalized.contains(req) || req.contains(normalized)))) {
                    continue;
                }
                String address = textOf(node, "addressDeposit");
                if (StringUtils.isNotBlank(address)) {
                    return address;
                }
            }
        }
        throw new ExchangeException("No deposit address returned for " + asset.toUpperCase() + " " + network);
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
        String candidate = network;
        int open = candidate.indexOf('(');
        int close = candidate.indexOf(')');
        if (open >= 0 && close > open) {
            candidate = candidate.substring(open + 1, close);
        }
        String n = candidate.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        return switch (n) {
            case "ETH", "ETHEREUM", "ERC20" -> "ETH";
            case "TRX", "TRON", "TRC20" -> "TRX";
            case "BSC", "BEP20", "BNBSMARTCHAIN" -> "BSC";
            case "ARBITRUM", "ARBITRUMONE", "ARB" -> "ARB";
            case "POLYGON", "MATIC" -> "MATIC";
            case "AVAXC", "AVALANCHECCHAIN" -> "AVAXC";
            case "SOL", "SOLANA" -> "SOL";
            case "OPTIMISM", "OP" -> "OP";
            default -> n;
        };
    }

    private SymbolMeta resolveSymbol(String base, String quote) {
        JsonNode result = requireOk(publicGet("/v5/market/instruments-info", Map.of("category", "spot")), "instruments info");
        JsonNode list = result == null ? null : result.get("list");
        if (list == null || !list.isArray()) {
            throw new ExchangeException("Unexpected response from Bybit instruments info API");
        }
        for (JsonNode s : list) {
            String b = textOf(s, "baseCoin");
            String q = textOf(s, "quoteCoin");
            String status = textOf(s, "status");
            if (StringUtils.isNotBlank(status) && !"Trading".equalsIgnoreCase(status)) {
                continue;
            }
            if (!base.equalsIgnoreCase(b) || !quote.equalsIgnoreCase(q)) {
                continue;
            }
            String symbol = textOf(s, "symbol");
            JsonNode lot = s.get("lotSizeFilter");
            BigDecimal minQty = dec(lot == null ? null : lot.get("minOrderQty"));
            BigDecimal step = dec(lot == null ? null : lot.get("basePrecision"));
            if (step.signum() <= 0) {
                step = dec(lot == null ? null : lot.get("qtyStep"));
            }
            BigDecimal minNotional = dec(lot == null ? null : lot.get("minOrderAmt"));
            return new SymbolMeta(symbol, minQty, step, minNotional);
        }
        throw new ExchangeException("Invalid symbol: " + base.toUpperCase() + "/" + quote.toUpperCase());
    }

    private CoinMeta resolveCoin(String asset) {
        JsonNode result = requireOk(signedGet("/v5/asset/coin/query-info", Map.of("coin", asset.toUpperCase())), "coin info");
        JsonNode rows = result == null ? null : result.get("rows");
        if (rows == null || !rows.isArray()) {
            throw new ExchangeException("Unexpected response from Bybit coin info API");
        }
        for (JsonNode row : rows) {
            String coin = textOf(row, "coin");
            if (!asset.equalsIgnoreCase(coin)) {
                continue;
            }
            List<ChainMeta> chains = new ArrayList<>();
            JsonNode list = row.get("chains");
            if (list != null && list.isArray()) {
                for (JsonNode c : list) {
                    String chain = textOf(c, "chain");
                    String chainType = textOf(c, "chainType");
                    if (StringUtils.isBlank(chain)) {
                        continue;
                    }
                    boolean dep = "1".equals(StringUtils.defaultString(textOf(c, "chainDeposit"), "1"));
                    boolean wd = "1".equals(StringUtils.defaultString(textOf(c, "chainWithdraw"), "1"));
                    chains.add(new ChainMeta(StringUtils.upperCase(chain), StringUtils.upperCase(chainType), dep, wd));
                }
            }
            return new CoinMeta(StringUtils.upperCase(coin), chains);
        }
        throw new ExchangeException("Bybit does not support asset: " + asset.toUpperCase());
    }

    private ChainMeta selectChain(CoinMeta coin, String requested, boolean withdraw) {
        List<ChainMeta> candidates = new ArrayList<>();
        for (ChainMeta c : coin.chains) {
            if (withdraw ? c.withdrawEnabled : c.depositEnabled) {
                candidates.add(c);
            }
        }
        if (candidates.isEmpty() && !coin.chains.isEmpty()) {
            candidates.addAll(coin.chains);
        }
        if (candidates.isEmpty()) {
            throw new ExchangeException("No " + (withdraw ? "withdraw" : "deposit") + " networks for asset " + coin.coin);
        }
        String req = normalizeDepositNetwork(requested);
        if (StringUtils.isBlank(req)) {
            if (candidates.size() == 1) {
                return candidates.get(0);
            }
            StringBuilder available = new StringBuilder();
            for (ChainMeta c : candidates) {
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(c.chain);
            }
            throw new ExchangeException("Network is required for " + coin.coin + " (available: " + available + ")");
        }
        for (ChainMeta c : candidates) {
            String chainNorm = normalizeDepositNetwork(c.chain);
            String typeNorm = normalizeDepositNetwork(c.chainType);
            if (req.equals(chainNorm) || req.equals(typeNorm)) {
                return c;
            }
            if (StringUtils.isNotBlank(chainNorm) && (chainNorm.contains(req) || req.contains(chainNorm))) {
                return c;
            }
            if (StringUtils.isNotBlank(typeNorm) && (typeNorm.contains(req) || req.contains(typeNorm))) {
                return c;
            }
        }
        throw new ExchangeException("Bybit does not support network " + requested + " for asset " + coin.coin);
    }

    private BigDecimal priceOf(String symbol) {
        JsonNode result = requireOk(publicGet("/v5/market/tickers", Map.of("category", "spot", "symbol", symbol)), "tickers");
        JsonNode list = result == null ? null : result.get("list");
        if (list != null && list.isArray() && !list.isEmpty()) {
            JsonNode t = list.get(0);
            BigDecimal ask = dec(t.get("ask1Price"));
            if (ask.signum() > 0) {
                return ask;
            }
            BigDecimal last = dec(t.get("lastPrice"));
            if (last.signum() > 0) {
                return last;
            }
        }
        return BigDecimal.ZERO;
    }

    private BuyInfoResult impact(String symbol, BigDecimal quoteAmount, JsonNode levels, boolean buy) {
        if (levels == null || !levels.isArray()) {
            throw new ExchangeException("Unexpected response from Bybit orderbook API");
        }
        BigDecimal rem = quoteAmount;
        BigDecimal quote = BigDecimal.ZERO;
        BigDecimal base = BigDecimal.ZERO;
        List<BuyInfoItem> items = new ArrayList<>();
        for (JsonNode level : levels) {
            if (!level.isArray() || level.size() < 2) {
                continue;
            }
            BigDecimal p = dec(level.get(0));
            BigDecimal q = dec(level.get(1));
            if (p.signum() <= 0 || q.signum() <= 0) {
                continue;
            }
            BigDecimal v = p.multiply(q);
            if (rem.compareTo(v) >= 0) {
                base = base.add(q);
                quote = quote.add(v);
                rem = rem.subtract(v);
                items.add(new BuyInfoItem(p, q, v));
            } else {
                BigDecimal part = rem.divide(p, 18, RoundingMode.DOWN);
                if (part.signum() > 0) {
                    BigDecimal pv = part.multiply(p);
                    base = base.add(part);
                    quote = quote.add(pv);
                    items.add(new BuyInfoItem(p, part, pv));
                }
                rem = BigDecimal.ZERO;
                break;
            }
        }
        if (base.signum() <= 0) {
            throw new ExchangeException("No " + (buy ? "ask" : "bid") + " liquidity available for " + symbol);
        }
        return new BuyInfoResult(symbol, quoteAmount, quote, base, quote.divide(base, 18, RoundingMode.HALF_UP), List.copyOf(items));
    }

    private List<OrderBookEntry> parseSide(JsonNode levels) {
        List<OrderBookEntry> out = new ArrayList<>();
        if (levels == null || !levels.isArray()) {
            return out;
        }
        for (JsonNode l : levels) {
            if (!l.isArray() || l.size() < 2) {
                continue;
            }
            BigDecimal p = dec(l.get(0));
            BigDecimal q = dec(l.get(1));
            if (p.signum() > 0 && q.signum() > 0) {
                out.add(new OrderBookEntry(p, q));
            }
        }
        return out;
    }

    private BigDecimal applyStep(BigDecimal value, BigDecimal step) {
        if (value == null || value.signum() <= 0) {
            return BigDecimal.ZERO;
        }
        if (step == null || step.signum() <= 0) {
            return value;
        }
        BigDecimal units = value.divide(step, 0, RoundingMode.DOWN);
        return step.multiply(units).stripTrailingZeros();
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = buildUri(path, params);
        LOG.info("bybit GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Bybit request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "public GET");
    }

    private JsonNode signedGet(String path, Map<String, String> params) {
        String query = qs(new TreeMap<>(params == null ? Map.of() : params));
        String uri = query.isEmpty() ? path : path + "?" + query;
        Auth auth = sign("GET", path, query, "");
        LOG.info("bybit GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-BAPI-API-KEY", apiKey())
                .header("X-BAPI-SIGN", auth.signature)
                .header("X-BAPI-TIMESTAMP", auth.timestamp)
                .header("X-BAPI-RECV-WINDOW", RECV_WINDOW)
                .header("X-BAPI-SIGN-TYPE", "2")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Bybit request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "signed GET");
    }

    private JsonNode signedPost(String path, Map<String, Object> payload) {
        String bodyJson = toJson(payload);
        Auth auth = sign("POST", path, "", bodyJson);
        LOG.info("bybit POST {}", LogSanitizer.sanitize(path + "?" + bodyJson));
        String body = webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header("X-BAPI-API-KEY", apiKey())
                .header("X-BAPI-SIGN", auth.signature)
                .header("X-BAPI-TIMESTAMP", auth.timestamp)
                .header("X-BAPI-RECV-WINDOW", RECV_WINDOW)
                .header("X-BAPI-SIGN-TYPE", "2")
                .bodyValue(bodyJson)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Bybit request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "signed POST");
    }

    private JsonNode requireOk(JsonNode root, String context) {
        if (root == null || root.isNull()) {
            throw new ExchangeException("Unexpected response from Bybit " + context + " API");
        }
        String code = textOf(root, "retCode");
        if (StringUtils.isNotBlank(code) && !"0".equals(code)) {
            String msg = textOf(root, "retMsg");
            throw new ExchangeException("Bybit " + context + " failed: code=" + code + " " + StringUtils.defaultString(msg));
        }
        return root.get("result");
    }

    private String buildUri(String path, Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return path;
        }
        return path + "?" + qs(params);
    }

    private String qs(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }
            if (!sb.isEmpty()) {
                sb.append("&");
            }
            sb.append(enc(e.getKey())).append("=").append(enc(e.getValue()));
        }
        return sb.toString();
    }

    private String enc(String s) {
        try {
            return URLEncoder.encode(s, StandardCharsets.UTF_8.toString()).replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode Bybit query parameter", e);
        }
    }

    private Auth sign(String method, String path, String query, String body) {
        String ts = String.valueOf(System.currentTimeMillis());
        String payload = ts + apiKey() + RECV_WINDOW + ("GET".equalsIgnoreCase(method) ? StringUtils.defaultString(query) : StringUtils.defaultString(body));
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(apiSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                hex.append(String.format("%02x", b));
            }
            return new Auth(ts, hex.toString());
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign Bybit request", e);
        }
    }

    private JsonNode readJson(String body, String context) {
        try {
            return MAPPER.readTree(StringUtils.defaultIfBlank(body, "{}"));
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse Bybit " + context + " response", e);
        }
    }

    private String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize Bybit request", e);
        }
    }

    private String apiKey() {
        String k = secrets == null ? null : secrets.getApiKey();
        if (StringUtils.isBlank(k)) {
            throw new ExchangeException("Missing API credentials for bybit");
        }
        return k;
    }

    private String apiSecret() {
        String s = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(s)) {
            throw new ExchangeException("Missing API credentials for bybit");
        }
        return s;
    }

    private String textOf(JsonNode n, String... keys) {
        if (n == null || keys == null) {
            return null;
        }
        for (String k : keys) {
            if (n.hasNonNull(k)) {
                String v = n.get(k).asText();
                if (StringUtils.isNotBlank(v)) {
                    return v;
                }
            }
        }
        return null;
    }

    private BigDecimal dec(JsonNode n) {
        if (n == null || n.isNull()) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(n.asText("0"));
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private record Auth(String timestamp, String signature) {
    }

    private record SymbolMeta(String symbol, BigDecimal minQty, BigDecimal qtyStep, BigDecimal minNotional) {
    }

    private record ChainMeta(String chain, String chainType, boolean depositEnabled, boolean withdrawEnabled) {
    }

    private record CoinMeta(String coin, List<ChainMeta> chains) {
    }
}
