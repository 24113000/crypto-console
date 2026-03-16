package com.crypto.console.exchanges.bitrue;

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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class BitrueClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {

    public BitrueClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("bitrue", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        JsonNode account = signedGet("/api/v1/account", Map.of());
        if (account == null || !account.has("balances") || !account.get("balances").isArray()) {
            throw new ExchangeException("Unexpected response from Bitrue account API");
        }
        for (JsonNode b : account.get("balances")) {
            String coin = textOf(b, "asset");
            if (asset.equalsIgnoreCase(coin)) {
                return new Balance(StringUtils.upperCase(coin), dec(b.get("free")), dec(b.get("locked")));
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("exchangeInfo coins[].chainDetail[].withdrawFee");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolInfo s = resolveSymbol(base, quote);
        int limit = depth <= 0 ? 100 : Math.min(1000, depth);
        JsonNode ob = publicGet("/api/v1/depth", Map.of("symbol", s.symbol, "limit", String.valueOf(limit)));
        if (ob == null) {
            throw new ExchangeException("Unexpected response from Bitrue depth API");
        }
        return new OrderBook(s.symbol, parseSide(ob.get("bids")), parseSide(ob.get("asks")));
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        JsonNode ob = publicGet("/api/v1/depth", Map.of("symbol", s.symbol, "limit", "1000"));
        return impact(s.symbol, quoteAmount, ob == null ? null : ob.get("asks"), true);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        JsonNode ob = publicGet("/api/v1/depth", Map.of("symbol", s.symbol, "limit", "1000"));
        return impact(s.symbol, quoteAmount, ob == null ? null : ob.get("bids"), false);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        if (s.minNotional.signum() > 0 && quoteAmount.compareTo(s.minNotional) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min notional " + s.minNotional + " for " + s.symbol);
        }
        Map<String, String> p = new LinkedHashMap<>();
        p.put("symbol", s.symbol);
        p.put("side", "BUY");
        p.put("type", "MARKET");
        p.put("quoteOrderQty", quoteAmount.toPlainString());
        p.put("newClientOrderId", "cc-" + UUID.randomUUID());
        JsonNode r = signedPost("/api/v1/order", p);
        String id = textOf(r, "orderId", "clientOrderId");
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing order id from Bitrue order API");
        }
        return new OrderResult(id, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        SymbolInfo s = resolveSymbol(base, quote);
        BigDecimal qty = applyStep(baseAmount, s.stepSize);
        if (s.minQty.signum() > 0 && qty.compareTo(s.minQty) < 0) {
            throw new ExchangeException("Sell quantity " + qty + " below min quantity " + s.minQty + " for " + s.symbol);
        }
        if (s.minNotional.signum() > 0) {
            BigDecimal p = lastPrice(s.symbol);
            if (p.signum() > 0 && qty.multiply(p).compareTo(s.minNotional) < 0) {
                throw new ExchangeException("Order value below min notional " + s.minNotional + " for " + s.symbol);
            }
        }
        Map<String, String> p = new LinkedHashMap<>();
        p.put("symbol", s.symbol);
        p.put("side", "SELL");
        p.put("type", "MARKET");
        p.put("quantity", qty.toPlainString());
        p.put("newClientOrderId", "cc-" + UUID.randomUUID());
        JsonNode r = signedPost("/api/v1/order", p);
        String id = textOf(r, "orderId", "clientOrderId");
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing order id from Bitrue order API");
        }
        return new OrderResult(id, "SUBMITTED", "market sell submitted");
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

        CoinInfo coin = resolveCoin(asset);
        ChainInfo chain = selectChain(coin, network, true);

        Map<String, String> p = new LinkedHashMap<>();
        p.put("coin", coin.coin);
        p.put("amount", amount.toPlainString());
        p.put("addressTo", address);
        p.put("chainName", chain.chain);
        if (StringUtils.isNotBlank(memoOrNull)) {
            p.put("tag", memoOrNull);
        }
        JsonNode r = signedPost("/api/v1/withdraw/commit", p);
        String withdrawId = textOf(r, "withdrawId", "id");
        if (StringUtils.isBlank(withdrawId)) {
            throw new ExchangeException("Missing withdrawal id from Bitrue withdraw API");
        }
        return new WithdrawResult(withdrawId, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode r = publicGet("/api/v1/time", Map.of());
        long t = r == null ? 0L : r.path("serverTime").asLong(0L);
        if (t <= 0L) {
            t = System.currentTimeMillis();
        }
        return new ExchangeTime(t, t - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        CoinInfo c = resolveCoin(asset);
        Set<String> out = new HashSet<>();
        for (ChainInfo ch : c.chains) {
            if (ch.depositEnabled) {
                out.add(StringUtils.upperCase(ch.chain));
            }
        }
        if (out.isEmpty()) {
            throw new ExchangeException("Bitrue does not support deposits for asset: " + asset.toUpperCase());
        }
        return out;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        CoinInfo c = resolveCoin(asset);
        ChainInfo ch = selectChain(c, network, false);
        JsonNode r = fetchDepositAddress(c.coin, ch);
        String addr = extractAddress(r, ch.chain);
        if (StringUtils.isBlank(addr)) {
            JsonNode history = signedGet("/api/v1/deposit/history", Map.of("coin", c.coin, "status", "1", "limit", "100"));
            addr = extractAddress(history, ch.chain);
        }
        if (StringUtils.isBlank(addr)) {
            throw new ExchangeException("No deposit address returned for " + asset.toUpperCase() + " " + network);
        }
        return addr;
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
            case "ARB", "ARBITRUM", "ARBITRUMONE" -> "ARB";
            case "POLYGON", "MATIC" -> "MATIC";
            case "AVAXC", "AVALANCHECCHAIN" -> "AVAXC";
            case "SOL", "SOLANA" -> "SOL";
            default -> n;
        };
    }

    private SymbolInfo resolveSymbol(String base, String quote) {
        JsonNode info = publicGet("/api/v1/exchangeInfo", Map.of());
        JsonNode symbols = info == null ? null : info.get("symbols");
        if (symbols == null || !symbols.isArray()) {
            throw new ExchangeException("Unexpected response from Bitrue exchangeInfo API");
        }
        for (JsonNode s : symbols) {
            String b = textOf(s, "baseAsset");
            String q = textOf(s, "quoteAsset");
            if (!base.equalsIgnoreCase(b) || !quote.equalsIgnoreCase(q)) {
                continue;
            }
            String symbol = textOf(s, "symbol");
            BigDecimal minQty = BigDecimal.ZERO;
            BigDecimal stepSize = BigDecimal.ZERO;
            BigDecimal minNotional = BigDecimal.ZERO;
            JsonNode filters = s.get("filters");
            if (filters != null && filters.isArray()) {
                for (JsonNode f : filters) {
                    String type = textOf(f, "filterType");
                    if ("LOT_SIZE".equalsIgnoreCase(type)) {
                        minQty = dec(f.get("minQty"));
                        stepSize = dec(f.get("stepSize"));
                    } else if ("MIN_NOTIONAL".equalsIgnoreCase(type) || "NOTIONAL".equalsIgnoreCase(type)) {
                        minNotional = dec(f.get("minNotional"));
                    }
                }
            }
            return new SymbolInfo(symbol, minQty, stepSize, minNotional);
        }
        throw new ExchangeException("Invalid symbol: " + base.toUpperCase() + "/" + quote.toUpperCase());
    }

    private CoinInfo resolveCoin(String asset) {
        JsonNode info = publicGet("/api/v1/exchangeInfo", Map.of());
        JsonNode coins = info == null ? null : info.get("coins");
        if (coins == null || !coins.isArray()) {
            throw new ExchangeException("Unexpected response from Bitrue exchangeInfo API");
        }
        for (JsonNode coinNode : coins) {
            String coin = textOf(coinNode, "coin");
            if (!asset.equalsIgnoreCase(coin)) {
                continue;
            }
            List<ChainInfo> chains = new ArrayList<>();
            JsonNode chainDetail = coinNode.get("chainDetail");
            if (chainDetail != null && chainDetail.isArray()) {
                for (JsonNode ch : chainDetail) {
                    String chain = textOf(ch, "chain");
                    if (StringUtils.isBlank(chain)) {
                        continue;
                    }
                    String originCoin = textOf(ch, "originCoin");
                    boolean dep = boolWithDefault(ch, true, "enableDeposit");
                    boolean wd = boolWithDefault(ch, true, "enableWithdraw");
                    chains.add(new ChainInfo(chain, StringUtils.upperCase(originCoin), dep, wd));
                }
            }
            return new CoinInfo(StringUtils.upperCase(coin), chains);
        }
        throw new ExchangeException("Bitrue does not support asset: " + asset.toUpperCase());
    }

    private ChainInfo selectChain(CoinInfo coin, String requested, boolean withdraw) {
        List<ChainInfo> candidates = new ArrayList<>();
        for (ChainInfo ch : coin.chains) {
            if (withdraw ? ch.withdrawEnabled : ch.depositEnabled) {
                candidates.add(ch);
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
            StringBuilder sb = new StringBuilder();
            for (ChainInfo c : candidates) {
                if (!sb.isEmpty()) {
                    sb.append(", ");
                }
                sb.append(c.chain);
            }
            throw new ExchangeException("Network is required for " + coin.coin + " (available: " + sb + ")");
        }
        for (ChainInfo c : candidates) {
            String cn = normalizeDepositNetwork(c.chain);
            if (req.equals(cn) || (StringUtils.isNotBlank(cn) && (cn.contains(req) || req.contains(cn)))) {
                return c;
            }
        }
        throw new ExchangeException("Bitrue does not support network " + requested + " for asset " + coin.coin);
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = buildUri(path, params);
        LOG.info("bitrue GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Bitrue request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
    }

    private JsonNode signedGet(String path, Map<String, String> params) {
        String query = signedQuery(params);
        String signature = sign(query, apiSecret());
        String uri = path + "?" + query + "&signature=" + signature;
        LOG.info("bitrue GET {}", LogSanitizer.sanitize(uri));
        JsonNode r = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Bitrue request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        assertNoApiError(r, "signed GET");
        return r;
    }

    private JsonNode signedPost(String path, Map<String, String> params) {
        String query = signedQuery(params);
        String signature = sign(query, apiSecret());
        String uri = path + "?" + query + "&signature=" + signature;
        LOG.info("bitrue POST {}", LogSanitizer.sanitize(uri));
        JsonNode r = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-MBX-APIKEY", apiKey())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Bitrue request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        assertNoApiError(r, "signed POST");
        return r;
    }

    private String signedQuery(Map<String, String> params) {
        Map<String, String> all = new LinkedHashMap<>();
        if (params != null) {
            all.putAll(params);
        }
        all.put("timestamp", String.valueOf(System.currentTimeMillis()));
        all.put("recvWindow", "5000");
        UriComponentsBuilder b = UriComponentsBuilder.newInstance();
        for (Map.Entry<String, String> e : all.entrySet()) {
            if (e.getValue() != null) {
                b.queryParam(e.getKey(), e.getValue());
            }
        }
        String q = b.build(true).getQuery();
        return q == null ? "" : q;
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

    private void assertNoApiError(JsonNode r, String context) {
        if (r == null) {
            throw new ExchangeException("Unexpected response from Bitrue " + context + " API");
        }
        if (r.has("code") && r.get("code").asInt(0) < 0) {
            String msg = textOf(r, "msg", "message");
            throw new ExchangeException("Bitrue " + context + " failed: code=" + r.get("code").asText() + " " + StringUtils.defaultString(msg));
        }
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
            throw new ExchangeException("Failed to sign Bitrue request", e);
        }
    }

    private BuyInfoResult impact(String symbol, BigDecimal quoteAmount, JsonNode levels, boolean buy) {
        if (levels == null || !levels.isArray()) {
            throw new ExchangeException("Unexpected response from Bitrue depth API");
        }
        BigDecimal rem = quoteAmount;
        BigDecimal quote = BigDecimal.ZERO;
        BigDecimal base = BigDecimal.ZERO;
        List<BuyInfoItem> items = new ArrayList<>();
        for (JsonNode l : levels) {
            if (!l.isArray() || l.size() < 2) {
                continue;
            }
            BigDecimal p = dec(l.get(0));
            BigDecimal q = dec(l.get(1));
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

    private List<OrderBookEntry> parseSide(JsonNode side) {
        List<OrderBookEntry> out = new ArrayList<>();
        if (side == null || !side.isArray()) {
            return out;
        }
        for (JsonNode l : side) {
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

    private BigDecimal lastPrice(String symbol) {
        JsonNode t = publicGet("/api/v1/ticker/price", Map.of("symbol", symbol));
        return dec(t == null ? null : t.get("price"));
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

    private boolean boolWithDefault(JsonNode node, boolean defaultValue, String key) {
        if (node == null || !node.has(key)) {
            return defaultValue;
        }
        JsonNode v = node.get(key);
        if (v.isBoolean()) {
            return v.asBoolean(defaultValue);
        }
        String t = StringUtils.trimToEmpty(v.asText()).toLowerCase();
        if ("true".equals(t) || "1".equals(t) || "yes".equals(t) || "enabled".equals(t)) {
            return true;
        }
        if ("false".equals(t) || "0".equals(t) || "no".equals(t) || "disabled".equals(t)) {
            return false;
        }
        return defaultValue;
    }

    private JsonNode fetchDepositAddress(String coin, ChainInfo chain) {
        Map<String, String> pCoinChainName = Map.of("coin", coin, "chainName", chain.chain);
        Map<String, String> pCoinChain = Map.of("coin", coin, "chain", chain.chain);
        Map<String, String> pCoinOnly = Map.of("coin", coin);
        Map<String, String> pOriginOnly = StringUtils.isNotBlank(chain.originCoin) ? Map.of("coin", chain.originCoin) : pCoinOnly;
        Map<String, String> pOriginChainName = StringUtils.isNotBlank(chain.originCoin) ? Map.of("coin", chain.originCoin, "chainName", chain.chain) : pCoinChainName;
        Map<String, String> pOriginChain = StringUtils.isNotBlank(chain.originCoin) ? Map.of("coin", chain.originCoin, "chain", chain.chain) : pCoinChain;
        List<Attempt> attempts = List.of(
                new Attempt("POST", "/api/v1/deposit/address", pCoinChainName),
                new Attempt("GET", "/api/v1/deposit/address", pCoinChainName),
                new Attempt("POST", "/api/v1/deposit/address", pCoinChain),
                new Attempt("GET", "/api/v1/deposit/address", pCoinChain),
                new Attempt("POST", "/api/v1/deposit/address", pCoinOnly),
                new Attempt("GET", "/api/v1/deposit/address", pCoinOnly),
                new Attempt("POST", "/api/v1/deposit/address", pOriginChainName),
                new Attempt("GET", "/api/v1/deposit/address", pOriginChainName),
                new Attempt("POST", "/api/v1/deposit/address", pOriginChain),
                new Attempt("GET", "/api/v1/deposit/address", pOriginChain),
                new Attempt("POST", "/api/v1/deposit/address", pOriginOnly),
                new Attempt("GET", "/api/v1/deposit/address", pOriginOnly),
                new Attempt("POST", "/api/v1/depositAddress", pCoinChainName),
                new Attempt("GET", "/api/v1/depositAddress", pCoinChainName),
                new Attempt("POST", "/api/v1/depositAddress", pCoinChain),
                new Attempt("GET", "/api/v1/depositAddress", pCoinChain),
                new Attempt("POST", "/api/v1/depositAddress", pCoinOnly),
                new Attempt("GET", "/api/v1/depositAddress", pCoinOnly),
                new Attempt("POST", "/api/v1/depositAddress", pOriginChainName),
                new Attempt("GET", "/api/v1/depositAddress", pOriginChainName),
                new Attempt("POST", "/api/v1/depositAddress", pOriginChain),
                new Attempt("GET", "/api/v1/depositAddress", pOriginChain),
                new Attempt("POST", "/api/v1/depositAddress", pOriginOnly),
                new Attempt("GET", "/api/v1/depositAddress", pOriginOnly)
        );
        ExchangeException last = null;
        for (Attempt a : attempts) {
            try {
                return "POST".equals(a.method) ? signedPost(a.path, a.params) : signedGet(a.path, a.params);
            } catch (ExchangeException ex) {
                last = ex;
                String m = StringUtils.defaultString(ex.getUserMessage());
                if (m.contains("HTTP 405") || m.contains("HTTP 404") || m.contains("HTTP 400") || m.contains("HTTP 412")) {
                    continue;
                }
                throw ex;
            }
        }
        if (last != null) {
            throw last;
        }
        return null;
    }

    private String extractAddress(JsonNode root, String requestedChain) {
        if (root == null || root.isNull()) {
            return null;
        }
        if (root.isTextual()) {
            return StringUtils.trimToNull(root.asText());
        }
        String direct = textOf(root, "address", "addressTo");
        if (StringUtils.isNotBlank(direct)) {
            return direct;
        }
        JsonNode data = root.get("data");
        String fromData = extractAddress(data, requestedChain);
        if (StringUtils.isNotBlank(fromData)) {
            return fromData;
        }
        if (root.isArray()) {
            String requested = normalizeDepositNetwork(requestedChain);
            for (JsonNode item : root) {
                String chain = textOf(item, "chain", "chainName", "network");
                if (StringUtils.isNotBlank(chain) && StringUtils.isNotBlank(requested)) {
                    String normalized = normalizeDepositNetwork(chain);
                    if (!requested.equals(normalized) && !(normalized != null && (normalized.contains(requested) || requested.contains(normalized)))) {
                        continue;
                    }
                }
                String addr = textOf(item, "address", "addressTo");
                if (StringUtils.isNotBlank(addr)) {
                    return addr;
                }
            }
            for (JsonNode item : root) {
                String addr = textOf(item, "address", "addressTo");
                if (StringUtils.isNotBlank(addr)) {
                    return addr;
                }
            }
        }
        return null;
    }

    private String apiKey() {
        String k = secrets == null ? null : secrets.getApiKey();
        if (StringUtils.isBlank(k)) {
            throw new ExchangeException("Missing API credentials for bitrue");
        }
        return k;
    }

    private String apiSecret() {
        String s = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(s)) {
            throw new ExchangeException("Missing API credentials for bitrue");
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

    private record SymbolInfo(String symbol, BigDecimal minQty, BigDecimal stepSize, BigDecimal minNotional) {
    }

    private record ChainInfo(String chain, String originCoin, boolean depositEnabled, boolean withdrawEnabled) {
    }

    private record CoinInfo(String coin, List<ChainInfo> chains) {
    }

    private record Attempt(String method, String path, Map<String, String> params) {
    }
}
