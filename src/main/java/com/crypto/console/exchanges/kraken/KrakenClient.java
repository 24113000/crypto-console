package com.crypto.console.exchanges.kraken;

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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Slf4j
public class KrakenClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public KrakenClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("kraken", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        JsonNode result = privatePost("/0/private/Balance", Map.of());
        String wanted = normalizeAssetCode(asset);
        BigDecimal total = BigDecimal.ZERO;
        Iterator<Map.Entry<String, JsonNode>> fields = result.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            String k = normalizeAssetCode(e.getKey());
            if (wanted.equals(k)) {
                total = total.add(dec(e.getValue()));
            }
        }
        return new Balance(asset.toUpperCase(), total, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("POST /0/private/WithdrawInfo requires key and amount");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolMeta s = resolveSymbol(base, quote);
        int count = depth <= 0 ? 100 : Math.min(500, depth);
        JsonNode result = publicGet("/0/public/Depth", Map.of("pair", s.altName, "count", String.valueOf(count)));
        JsonNode book = firstBook(result);
        return new OrderBook(s.altName, parseSide(book.get("bids")), parseSide(book.get("asks")));
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        JsonNode result = publicGet("/0/public/Depth", Map.of("pair", s.altName, "count", "500"));
        JsonNode book = firstBook(result);
        return impact(s.altName, quoteAmount, book.get("asks"), true);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        JsonNode result = publicGet("/0/public/Depth", Map.of("pair", s.altName, "count", "500"));
        JsonNode book = firstBook(result);
        return impact(s.altName, quoteAmount, book.get("bids"), false);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        if (s.minNotional.signum() > 0 && quoteAmount.compareTo(s.minNotional) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min notional " + s.minNotional + " for " + s.altName);
        }
        Map<String, String> params = new LinkedHashMap<>();
        params.put("pair", s.altName);
        params.put("type", "buy");
        params.put("ordertype", "market");
        params.put("volume", quoteAmount.toPlainString());
        params.put("oflags", "viqc");
        JsonNode result = privatePost("/0/private/AddOrder", params);
        String txid = extractTxid(result);
        return new OrderResult(txid, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        BigDecimal qty = applyStep(baseAmount, s.stepSize);
        if (s.minQty.signum() > 0 && qty.compareTo(s.minQty) < 0) {
            throw new ExchangeException("Sell quantity " + qty + " below min quantity " + s.minQty + " for " + s.altName);
        }
        if (s.minNotional.signum() > 0) {
            BigDecimal p = lastPrice(s.altName);
            if (p.signum() > 0 && qty.multiply(p).compareTo(s.minNotional) < 0) {
                throw new ExchangeException("Order value below min notional " + s.minNotional + " for " + s.altName);
            }
        }
        Map<String, String> params = new LinkedHashMap<>();
        params.put("pair", s.altName);
        params.put("type", "sell");
        params.put("ordertype", "market");
        params.put("volume", qty.toPlainString());
        JsonNode result = privatePost("/0/private/AddOrder", params);
        String txid = extractTxid(result);
        return new OrderResult(txid, "SUBMITTED", "market sell submitted");
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
        String method = selectWithdrawMethod(asset, network);
        String key = resolveWithdrawalKey(asset, method, address);
        if (StringUtils.isBlank(key)) {
            throw new ExchangeException("Kraken requires a pre-saved withdrawal key. No key found for address " + address + " on method " + method);
        }
        Map<String, String> params = new LinkedHashMap<>();
        params.put("asset", normalizeAssetCode(asset));
        params.put("key", key);
        params.put("amount", amount.toPlainString());
        if (StringUtils.isNotBlank(memoOrNull)) {
            params.put("address_tag", memoOrNull);
        }
        JsonNode result = privatePost("/0/private/Withdraw", params);
        String refid = textOf(result, "refid");
        if (StringUtils.isBlank(refid)) {
            throw new ExchangeException("Missing withdrawal id from Kraken withdraw API");
        }
        return new WithdrawResult(refid, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode result = publicGet("/0/public/Time", Map.of());
        long unixtime = result.path("unixtime").asLong(0L) * 1000L;
        if (unixtime <= 0L) {
            unixtime = System.currentTimeMillis();
        }
        return new ExchangeTime(unixtime, unixtime - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        JsonNode methods = privatePost("/0/private/DepositMethods", Map.of("asset", normalizeAssetCode(asset)));
        if (methods == null || !methods.isArray()) {
            throw new ExchangeException("Unexpected response from Kraken DepositMethods API");
        }
        Set<String> out = new LinkedHashSet<>();
        for (JsonNode m : methods) {
            String method = textOf(m, "method");
            if (StringUtils.isNotBlank(method)) {
                out.add(StringUtils.upperCase(method));
            }
        }
        if (out.isEmpty()) {
            throw new ExchangeException("Kraken does not support deposits for asset: " + asset.toUpperCase());
        }
        return out;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        String method = selectDepositMethod(asset, network);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("asset", normalizeAssetCode(asset));
        params.put("method", method);
        JsonNode addresses = privatePost("/0/private/DepositAddresses", params);
        String address = firstAddress(addresses);
        if (StringUtils.isBlank(address)) {
            params.put("new", "true");
            addresses = privatePost("/0/private/DepositAddresses", params);
            address = firstAddress(addresses);
        }
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
        String candidate = network;
        int open = candidate.indexOf('(');
        int close = candidate.indexOf(')');
        if (open >= 0 && close > open) {
            candidate = candidate.substring(open + 1, close);
        }
        String n = candidate.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        return switch (n) {
            case "ETH", "ETHEREUM", "ERC20" -> "ETH";
            case "TRX", "TRON", "TRC20" -> "TRON";
            case "BSC", "BEP20", "BNBSMARTCHAIN" -> "BSC";
            case "ARBITRUM", "ARBITRUMONE", "ARB" -> "ARBITRUM";
            case "POLYGON", "MATIC" -> "POLYGON";
            case "AVAXC", "AVALANCHECCHAIN" -> "AVAXC";
            case "SOL", "SOLANA" -> "SOLANA";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            default -> n;
        };
    }

    private SymbolMeta resolveSymbol(String base, String quote) {
        JsonNode result = publicGet("/0/public/AssetPairs", Map.of());
        if (result == null || !result.isObject()) {
            throw new ExchangeException("Unexpected response from Kraken AssetPairs API");
        }
        String baseWanted = normalizePairAsset(base);
        String quoteWanted = normalizePairAsset(quote);
        List<SymbolMeta> weakQuoteMatches = new ArrayList<>();
        List<String> similarMarkets = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> it = result.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            JsonNode pair = entry.getValue();
            String wsname = textOf(pair, "wsname");
            String altname = textOf(pair, "altname");
            String baseRaw = textOf(pair, "base");
            String quoteRaw = textOf(pair, "quote");
            String pBase = wsnameBase(wsname);
            String pQuote = wsnameQuote(wsname);
            if (StringUtils.isBlank(pBase)) {
                pBase = normalizePairAsset(baseRaw);
            }
            if (StringUtils.isBlank(pQuote)) {
                pQuote = normalizePairAsset(quoteRaw);
            }
            if (!baseWanted.equals(pBase)) {
                if (StringUtils.isNotBlank(pBase) && pBase.contains(baseWanted)) {
                    String shown = StringUtils.defaultIfBlank(wsname, StringUtils.defaultIfBlank(altname, entry.getKey()));
                    if (similarMarkets.size() < 8 && StringUtils.isNotBlank(shown)) {
                        similarMarkets.add(shown);
                    }
                }
                continue;
            }
            BigDecimal minQty = dec(pair.get("ordermin"));
            BigDecimal minNotional = dec(pair.get("costmin"));
            int lotDecimals = pair.path("lot_decimals").asInt(8);
            BigDecimal step = BigDecimal.ONE.scaleByPowerOfTen(-Math.max(lotDecimals, 0));
            SymbolMeta symbolMeta = new SymbolMeta(StringUtils.defaultIfBlank(altname, entry.getKey()), minQty, step, minNotional);
            if (quoteWanted.equals(pQuote)) {
                return symbolMeta;
            }
            if (isWeakUsdFallback(quoteWanted, pQuote)) {
                weakQuoteMatches.add(symbolMeta);
            }
        }
        if (weakQuoteMatches.size() == 1) {
            return weakQuoteMatches.get(0);
        }
        if (!weakQuoteMatches.isEmpty()) {
            StringBuilder options = new StringBuilder();
            for (int i = 0; i < Math.min(weakQuoteMatches.size(), 8); i++) {
                if (i > 0) {
                    options.append(", ");
                }
                options.append(weakQuoteMatches.get(i).altName);
            }
            throw new ExchangeException("Ambiguous USD fallback for " + base.toUpperCase() + "/" + quote.toUpperCase() + ". Candidates: " + options);
        }
        if (!similarMarkets.isEmpty()) {
            throw new ExchangeException("Invalid symbol: " + base.toUpperCase() + "/" + quote.toUpperCase() + ". Similar markets: " + String.join(", ", similarMarkets));
        }
        throw new ExchangeException("Invalid symbol: " + base.toUpperCase() + "/" + quote.toUpperCase());
    }

    private boolean isWeakUsdFallback(String requestedQuote, String pairQuote) {
        return "USDT".equalsIgnoreCase(requestedQuote) && "USD".equalsIgnoreCase(pairQuote);
    }

    private String selectDepositMethod(String asset, String network) {
        JsonNode methods = privatePost("/0/private/DepositMethods", Map.of("asset", normalizeAssetCode(asset)));
        return selectMethod(methods, network, "deposit", asset);
    }

    private String selectWithdrawMethod(String asset, String network) {
        JsonNode methods = privatePost("/0/private/WithdrawMethods", Map.of("asset", normalizeAssetCode(asset)));
        return selectMethod(methods, network, "withdraw", asset);
    }

    private String selectMethod(JsonNode methods, String requestedNetwork, String action, String asset) {
        if (methods == null || !methods.isArray()) {
            throw new ExchangeException("Unexpected response from Kraken " + ("deposit".equals(action) ? "DepositMethods" : "WithdrawMethods") + " API");
        }
        List<String> available = new ArrayList<>();
        String req = normalizeDepositNetwork(requestedNetwork);
        for (JsonNode methodNode : methods) {
            String method = textOf(methodNode, "method");
            if (StringUtils.isBlank(method)) {
                continue;
            }
            available.add(method);
            if (StringUtils.isBlank(req)) {
                continue;
            }
            String n = normalizeDepositNetwork(method);
            if (req.equals(n) || (n != null && (n.contains(req) || req.contains(n)))) {
                return method;
            }
        }
        if (StringUtils.isBlank(req)) {
            if (available.size() == 1) {
                return available.get(0);
            }
            throw new ExchangeException("Network is required for " + asset.toUpperCase() + " (available: " + String.join(", ", available) + ")");
        }
        throw new ExchangeException("Kraken does not support network " + requestedNetwork + " for asset " + asset.toUpperCase());
    }

    private String resolveWithdrawalKey(String asset, String method, String address) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("asset", normalizeAssetCode(asset));
        p.put("method", method);
        JsonNode response = privatePost("/0/private/WithdrawAddresses", p);
        if (response == null || !response.isArray()) {
            return null;
        }
        for (JsonNode row : response) {
            String addr = textOf(row, "address");
            if (!address.equalsIgnoreCase(StringUtils.defaultString(addr))) {
                continue;
            }
            String key = textOf(row, "key", "refid", "description", "label");
            if (StringUtils.isNotBlank(key)) {
                return key;
            }
        }
        return null;
    }

    private String firstAddress(JsonNode addresses) {
        if (addresses == null || !addresses.isArray()) {
            return null;
        }
        for (JsonNode item : addresses) {
            String address = textOf(item, "address");
            if (StringUtils.isNotBlank(address)) {
                return address;
            }
        }
        return null;
    }

    private String extractTxid(JsonNode result) {
        JsonNode txid = result.get("txid");
        if (txid != null && txid.isArray() && !txid.isEmpty()) {
            return txid.get(0).asText();
        }
        String descr = textOf(result, "descr", "order");
        throw new ExchangeException("Missing order id from Kraken AddOrder response" + (StringUtils.isBlank(descr) ? "" : ": " + descr));
    }

    private BigDecimal lastPrice(String altName) {
        JsonNode result = publicGet("/0/public/Ticker", Map.of("pair", altName));
        if (result == null || !result.isObject()) {
            return BigDecimal.ZERO;
        }
        Iterator<Map.Entry<String, JsonNode>> it = result.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode v = e.getValue();
            if (!v.isObject()) {
                continue;
            }
            JsonNode c = v.get("c");
            if (c != null && c.isArray() && !c.isEmpty()) {
                return dec(c.get(0));
            }
        }
        return BigDecimal.ZERO;
    }

    private BuyInfoResult impact(String symbol, BigDecimal quoteAmount, JsonNode levels, boolean buy) {
        if (levels == null || !levels.isArray()) {
            throw new ExchangeException("Unexpected response from Kraken order book API");
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
        for (JsonNode level : levels) {
            if (!level.isArray() || level.size() < 2) {
                continue;
            }
            BigDecimal p = dec(level.get(0));
            BigDecimal q = dec(level.get(1));
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

    private JsonNode firstBook(JsonNode result) {
        Iterator<Map.Entry<String, JsonNode>> it = result.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if ("last".equalsIgnoreCase(e.getKey())) {
                continue;
            }
            return e.getValue();
        }
        throw new ExchangeException("Unexpected response from Kraken depth API");
    }

    private String wsnameBase(String wsname) {
        if (StringUtils.isBlank(wsname) || !wsname.contains("/")) {
            return null;
        }
        return normalizePairAsset(wsname.split("/")[0]);
    }

    private String wsnameQuote(String wsname) {
        if (StringUtils.isBlank(wsname) || !wsname.contains("/")) {
            return null;
        }
        return normalizePairAsset(wsname.split("/")[1]);
    }

    private String normalizePairAsset(String asset) {
        String a = normalizeAssetCode(asset);
        return switch (a) {
            case "BTC" -> "XBT";
            case "XDG" -> "DOGE";
            default -> a;
        };
    }

    private String normalizeAssetCode(String asset) {
        if (StringUtils.isBlank(asset)) {
            return "";
        }
        String cleaned = asset.toUpperCase();
        int dot = cleaned.indexOf('.');
        if (dot > 0) {
            cleaned = cleaned.substring(0, dot);
        }
        String a = cleaned.replaceAll("[^A-Z0-9]", "");
        if (a.startsWith("X") || a.startsWith("Z")) {
            if ("XXBT".equals(a) || "XBT".equals(a)) return "XBT";
            if ("ZUSD".equals(a) || "USD".equals(a)) return "USD";
            if ("ZEUR".equals(a) || "EUR".equals(a)) return "EUR";
            if ("USDT".equals(a) || "ZUSDT".equals(a)) return "USDT";
            if ("XDG".equals(a) || "XXDG".equals(a) || "DOGE".equals(a)) return "DOGE";
        }
        return switch (a) {
            case "BTC" -> "XBT";
            default -> a;
        };
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = params == null || params.isEmpty() ? path : path + "?" + qs(params);
        LOG.info("kraken GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Kraken request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        JsonNode root = readJson(body, "public");
        ensureNoKrakenError(root, "public");
        JsonNode result = root.get("result");
        if (result == null) {
            throw new ExchangeException("Unexpected response from Kraken public API");
        }
        return result;
    }

    private JsonNode privatePost(String path, Map<String, String> params) {
        Map<String, String> body = new TreeMap<>();
        if (params != null) {
            body.putAll(params);
        }
        String nonce = String.valueOf(System.currentTimeMillis());
        body.put("nonce", nonce);
        String postData = qs(body);
        String signature = signKraken(path, nonce, postData);
        LOG.info("kraken POST {}", LogSanitizer.sanitize(path + "?" + postData));
        String resp = webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .header("API-Key", apiKey())
                .header("API-Sign", signature)
                .bodyValue(postData)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("Kraken request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        JsonNode root = readJson(resp, "private");
        ensureNoKrakenError(root, "private");
        JsonNode result = root.get("result");
        if (result == null) {
            throw new ExchangeException("Unexpected response from Kraken private API");
        }
        return result;
    }

    private void ensureNoKrakenError(JsonNode root, String context) {
        if (root == null) {
            throw new ExchangeException("Unexpected response from Kraken " + context + " API");
        }
        JsonNode errors = root.get("error");
        if (errors != null && errors.isArray() && !errors.isEmpty()) {
            List<String> all = new ArrayList<>();
            for (JsonNode e : errors) {
                if (e != null && StringUtils.isNotBlank(e.asText())) {
                    all.add(e.asText());
                }
            }
            if (!all.isEmpty()) {
                throw new ExchangeException("Kraken " + context + " failed: " + String.join(" | ", all));
            }
        }
    }

    private String signKraken(String path, String nonce, String postData) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] hash = sha256.digest((nonce + postData).getBytes(StandardCharsets.UTF_8));
            byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
            byte[] message = new byte[pathBytes.length + hash.length];
            System.arraycopy(pathBytes, 0, message, 0, pathBytes.length);
            System.arraycopy(hash, 0, message, pathBytes.length, hash.length);

            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(Base64.getDecoder().decode(apiSecret()), "HmacSHA512"));
            return Base64.getEncoder().encodeToString(mac.doFinal(message));
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign Kraken request", e);
        }
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
            sb.append(urlEncode(e.getKey())).append("=").append(urlEncode(e.getValue()));
        }
        return sb.toString();
    }

    private String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString()).replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode Kraken query param", e);
        }
    }

    private JsonNode readJson(String body, String context) {
        try {
            return MAPPER.readTree(StringUtils.defaultIfBlank(body, "{}"));
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse Kraken " + context + " response", e);
        }
    }

    private String apiKey() {
        String k = secrets == null ? null : secrets.getApiKey();
        if (StringUtils.isBlank(k)) {
            throw new ExchangeException("Missing API credentials for kraken");
        }
        return k;
    }

    private String apiSecret() {
        String s = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(s)) {
            throw new ExchangeException("Missing API credentials for kraken");
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

    private record SymbolMeta(String altName, BigDecimal minQty, BigDecimal stepSize, BigDecimal minNotional) {
    }

    private record ChainMeta(String method, boolean depositEnabled, boolean withdrawEnabled) {
    }
}
