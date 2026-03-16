package com.crypto.console.exchanges.bingx;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;
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
import java.util.*;

@Slf4j
public class BingxClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public BingxClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("bingx", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        JsonNode data = requireOk(signedGet("/openApi/spot/v1/account/balance", Map.of()), "balance");
        JsonNode balances = data.has("balances") ? data.get("balances") : data;
        if (balances != null && balances.isArray()) {
            for (JsonNode b : balances) {
                String coin = textOf(b, "asset", "coin", "currency");
                if (asset.equalsIgnoreCase(coin)) {
                    return new Balance(coin.toUpperCase(), dec(b.get("free")), dec(b.get("locked")));
                }
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /openApi/wallets/v1/capital/config/getall");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode d = requireOk(publicGet("/openApi/spot/v1/market/depth", Map.of("symbol", symbol.symbol, "limit", String.valueOf(Math.max(5, Math.min(depth, 1000))))), "depth");
        boolean priceFirst = detectPriceFirst(symbol.symbol, arr(d, "bids", "bid"), arr(d, "asks", "ask"));
        return new OrderBook(symbol.symbol, side(d, "bids", "bid", priceFirst), side(d, "asks", "ask", priceFirst));
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode d = requireOk(publicGet("/openApi/spot/v1/market/depth", Map.of("symbol", symbol.symbol, "limit", "1000")), "depth");
        JsonNode asks = arr(d, "asks", "ask");
        boolean priceFirst = detectPriceFirst(symbol.symbol, arr(d, "bids", "bid"), asks);
        return impact(symbol.symbol, quoteAmount, asks, true, priceFirst);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode d = requireOk(publicGet("/openApi/spot/v1/market/depth", Map.of("symbol", symbol.symbol, "limit", "1000")), "depth");
        JsonNode bids = arr(d, "bids", "bid");
        boolean priceFirst = detectPriceFirst(symbol.symbol, bids, arr(d, "asks", "ask"));
        return impact(symbol.symbol, quoteAmount, bids, false, priceFirst);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        SymbolMeta symbol = resolveSymbol(base, quote);
        Map<String, String> p = new LinkedHashMap<>();
        p.put("symbol", symbol.symbol);
        p.put("side", "BUY");
        p.put("type", "MARKET");
        p.put("quoteOrderQty", quoteAmount.toPlainString());
        JsonNode r;
        try {
            r = signedPost("/openApi/spot/v1/trade/order", p);
        } catch (ExchangeException ex) {
            BigDecimal price = ticker(symbol.symbol);
            BigDecimal qty = step(quoteAmount.divide(price, 18, RoundingMode.DOWN), symbol.step);
            p.remove("quoteOrderQty");
            p.put("quantity", qty.toPlainString());
            r = signedPost("/openApi/spot/v1/trade/order", p);
        }
        JsonNode d = requireOk(r, "order");
        return new OrderResult(textOf(d, "orderId", "id", "clientOrderId"), "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        SymbolMeta symbol = resolveSymbol(base, quote);
        BigDecimal qty = step(baseAmount, symbol.step);
        if (symbol.minNotional.signum() > 0) {
            BigDecimal notional = qty.multiply(ticker(symbol.symbol));
            if (notional.compareTo(symbol.minNotional) < 0) {
                throw new ExchangeException("Order value " + notional + " below min notional " + symbol.minNotional + " for " + symbol.symbol);
            }
        }
        Map<String, String> p = new LinkedHashMap<>();
        p.put("symbol", symbol.symbol);
        p.put("side", "SELL");
        p.put("type", "MARKET");
        p.put("quantity", qty.toPlainString());
        JsonNode d = requireOk(signedPost("/openApi/spot/v1/trade/order", p), "order");
        return new OrderResult(textOf(d, "orderId", "id", "clientOrderId"), "SUBMITTED", "market sell submitted");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        AssetMeta meta = resolveAsset(asset);
        String net = pickNetwork(meta, network, true);
        Map<String, String> p = new LinkedHashMap<>();
        p.put("coin", meta.coin);
        p.put("address", address);
        p.put("amount", amount.toPlainString());
        if (StringUtils.isNotBlank(net)) {
            p.put("network", net);
        }
        JsonNode d = requireOk(signedPost("/openApi/wallets/v1/capital/withdraw/apply", p), "withdraw");
        return new WithdrawResult(textOf(d, "id", "withdrawId", "orderId"), "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode d = requireOk(publicGet("/openApi/spot/v1/server/time", Map.of()), "server time");
        long t = lng(d, "serverTime", "timestamp", "time");
        if (t <= 0) t = System.currentTimeMillis();
        return new ExchangeTime(t, t - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        AssetMeta meta = resolveAsset(asset);
        Set<String> s = new LinkedHashSet<>();
        for (NetMeta n : meta.networks) if (n.deposit) s.add(n.name);
        if (s.isEmpty()) throw new ExchangeException("BingX does not support deposits for asset: " + asset.toUpperCase());
        return s;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        AssetMeta meta = resolveAsset(asset);
        String net = pickNetwork(meta, network, false);
        Map<String, String> p = new LinkedHashMap<>();
        p.put("coin", meta.coin);
        if (StringUtils.isNotBlank(net)) p.put("network", net);
        JsonNode d = requireOk(signedGet("/openApi/wallets/v1/capital/deposit/address", p), "deposit address");
        if (d.isArray()) {
            for (JsonNode i : d) if (StringUtils.isBlank(net) || norm(textOf(i, "network")).equals(norm(net))) return textOf(i, "address");
            return null;
        }
        return textOf(d, "address");
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, true, true, true, true, true);
    }

    @Override
    public String normalizeDepositNetwork(String network) {
        return norm(network);
    }

    private SymbolMeta resolveSymbol(String base, String quote) {
        JsonNode d = requireOk(publicGet("/openApi/spot/v1/common/symbols", Map.of()), "symbols");
        JsonNode syms = d.has("symbols") ? d.get("symbols") : d;
        List<SymbolMeta> pref = new ArrayList<>();
        for (JsonNode s : syms) {
            String symbol = textOf(s, "symbol");
            if (StringUtils.isBlank(symbol)) continue;
            String[] parts = split(symbol);
            if (parts == null) continue;
            if (parts[0].equalsIgnoreCase(base) && parts[1].equalsIgnoreCase(quote)) return parseSymbol(s, symbol);
            if (parts[1].equalsIgnoreCase(quote) && parts[0].toUpperCase().endsWith(base.toUpperCase()) && parts[0].length() > base.length()) {
                String prefix = parts[0].substring(0, parts[0].length() - base.length());
                if (prefix.chars().allMatch(Character::isDigit)) pref.add(parseSymbol(s, symbol));
            }
        }
        if (pref.size() == 1) return pref.get(0);
        throw new ExchangeException("Invalid symbol: " + base.toUpperCase() + "/" + quote.toUpperCase());
    }

    private SymbolMeta parseSymbol(JsonNode s, String symbol) {
        return new SymbolMeta(symbol, filter(s, "LOT_SIZE", "stepSize", "step", "quantityStep"), filter(s, "MIN_NOTIONAL", "minNotional", "minAmount", "notional"));
    }

    private BigDecimal filter(JsonNode s, String type, String... keys) {
        JsonNode fs = s.get("filters");
        if (fs != null && fs.isArray()) {
            for (JsonNode f : fs) if (type.equalsIgnoreCase(textOf(f, "filterType", "type"))) for (String k : keys) {
                BigDecimal v = dec(f.get(k)); if (v.signum() > 0) return v;
            }
        }
        for (String k : keys) { BigDecimal v = dec(s.get(k)); if (v.signum() > 0) return v; }
        return BigDecimal.ZERO;
    }

    private AssetMeta resolveAsset(String asset) {
        JsonNode data = requireOk(signedGet("/openApi/wallets/v1/capital/config/getall", Map.of()), "wallet config");
        for (JsonNode c : data) {
            String coin = textOf(c, "coin", "asset");
            if (!asset.equalsIgnoreCase(coin)) continue;
            List<NetMeta> nets = new ArrayList<>();
            JsonNode nl = c.has("networkList") ? c.get("networkList") : c.get("networks");
            if (nl != null && nl.isArray()) for (JsonNode n : nl) {
                String name = StringUtils.upperCase(textOf(n, "network", "chain", "name"));
                if (StringUtils.isBlank(name)) continue;
                nets.add(new NetMeta(name, bool(n, "depositEnable", "depositEnabled", "canDeposit"), bool(n, "withdrawEnable", "withdrawEnabled", "canWithdraw")));
            }
            return new AssetMeta(coin.toUpperCase(), nets);
        }
        throw new ExchangeException("BingX does not support asset: " + asset.toUpperCase());
    }

    private String pickNetwork(AssetMeta a, String requested, boolean wd) {
        List<NetMeta> c = new ArrayList<>();
        for (NetMeta n : a.networks) if ((wd && n.withdraw) || (!wd && n.deposit)) c.add(n);
        if (c.isEmpty()) throw new ExchangeException("BingX does not support " + (wd ? "withdrawal" : "deposit") + " for asset: " + a.coin);
        String r = norm(requested);
        if (StringUtils.isBlank(r)) {
            if (c.size() == 1) return c.get(0).name;
            StringBuilder sb = new StringBuilder();
            for (NetMeta n : c) { if (!sb.isEmpty()) sb.append(", "); sb.append(n.name); }
            throw new ExchangeException("Network is required for " + a.coin + " (available: " + sb + ")");
        }
        for (NetMeta n : c) if (norm(n.name).equals(r)) return n.name;
        throw new ExchangeException("BingX does not support network " + requested + " for asset " + a.coin);
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = uri(path, params);
        LOG.info("bingx GET {}", LogSanitizer.sanitize(uri));
        return json(webClient.get().uri(uri).header(HttpHeaders.USER_AGENT, "crypto-console").retrieve().bodyToMono(String.class).block(), "public");
    }

    private JsonNode signedGet(String path, Map<String, String> params) {
        String uri = signUri(path, params);
        LOG.info("bingx GET {}", LogSanitizer.sanitize(uri));
        return json(webClient.get().uri(uri).header(HttpHeaders.USER_AGENT, "crypto-console").header("X-BX-APIKEY", key()).retrieve().bodyToMono(String.class).block(), "signed GET");
    }

    private JsonNode signedPost(String path, Map<String, String> params) {
        String uri = signUri(path, params);
        LOG.info("bingx POST {}", LogSanitizer.sanitize(uri));
        return json(webClient.post().uri(uri).header(HttpHeaders.USER_AGENT, "crypto-console").header("X-BX-APIKEY", key()).retrieve().bodyToMono(String.class).block(), "signed POST");
    }

    private JsonNode requireOk(JsonNode r, String context) {
        if (r == null) throw new ExchangeException("Unexpected response from BingX " + context + " API");
        if (r.has("code")) {
            String c = r.get("code").asText();
            if (!"0".equals(c) && !"200".equals(c) && !c.isBlank()) throw new ExchangeException("BingX " + context + " failed: code=" + c + " " + StringUtils.defaultString(textOf(r, "msg", "message")));
        }
        if (r.has("success") && !r.get("success").asBoolean(true)) throw new ExchangeException("BingX " + context + " failed: " + StringUtils.defaultString(textOf(r, "msg", "message")));
        if (r.has("data")) {
            JsonNode d = r.get("data");
            return d != null && d.has("data") ? d.get("data") : d;
        }
        return r;
    }

    private BuyInfoResult impact(String symbol, BigDecimal qQuote, JsonNode levels, boolean buy, boolean priceFirst) {
        if (levels == null || !levels.isArray()) throw new ExchangeException("Unexpected response from BingX depth API");
        BigDecimal rem = qQuote, q = BigDecimal.ZERO, b = BigDecimal.ZERO;
        List<BuyInfoItem> aff = new ArrayList<>();
        for (JsonNode l : levels) {
            if (!l.isArray() || l.size() < 2) continue;
            BigDecimal p = dec(l.get(priceFirst ? 0 : 1));
            BigDecimal qty = dec(l.get(priceFirst ? 1 : 0));
            if (p.signum() <= 0 || qty.signum() <= 0) continue;
            BigDecimal v = p.multiply(qty);
            if (rem.compareTo(v) >= 0) {
                b = b.add(qty); q = q.add(v); rem = rem.subtract(v); aff.add(new BuyInfoItem(p, qty, v));
            } else {
                BigDecimal part = rem.divide(p, 18, RoundingMode.DOWN);
                if (part.signum() > 0) { BigDecimal pv = part.multiply(p); b = b.add(part); q = q.add(pv); aff.add(new BuyInfoItem(p, part, pv)); }
                rem = BigDecimal.ZERO; break;
            }
        }
        if (b.signum() <= 0) throw new ExchangeException("No " + (buy ? "ask" : "bid") + " liquidity available for " + symbol);
        return new BuyInfoResult(symbol, qQuote, q, b, q.divide(b, 18, RoundingMode.HALF_UP), List.copyOf(aff));
    }

    private List<OrderBookEntry> side(JsonNode d, String k1, String k2, boolean priceFirst) {
        List<OrderBookEntry> out = new ArrayList<>();
        JsonNode n = arr(d, k1, k2);
        if (n == null || !n.isArray()) return out;
        for (JsonNode i : n) if (i.isArray() && i.size() >= 2) {
            BigDecimal p = dec(i.get(priceFirst ? 0 : 1));
            BigDecimal q = dec(i.get(priceFirst ? 1 : 0));
            if (p.signum() > 0 && q.signum() > 0) out.add(new OrderBookEntry(p, q));
        }
        return out;
    }

    private boolean detectPriceFirst(String symbol, JsonNode bids, JsonNode asks) {
        BigDecimal ref = ticker(symbol);
        if (ref.signum() <= 0) return true;

        BigDecimal bid0 = firstValue(bids, 0);
        BigDecimal ask0 = firstValue(asks, 0);
        BigDecimal bid1 = firstValue(bids, 1);
        BigDecimal ask1 = firstValue(asks, 1);

        BigDecimal s0 = orientationScore(bid0, ask0, ref);
        BigDecimal s1 = orientationScore(bid1, ask1, ref);
        return s0.compareTo(s1) <= 0;
    }

    private BigDecimal orientationScore(BigDecimal bid, BigDecimal ask, BigDecimal ref) {
        BigDecimal score = BigDecimal.ZERO;
        if (bid.signum() > 0) score = score.add(relativeDiff(bid, ref));
        if (ask.signum() > 0) score = score.add(relativeDiff(ask, ref));
        if (bid.signum() > 0 && ask.signum() > 0 && ask.compareTo(bid) < 0) {
            score = score.add(BigDecimal.valueOf(1000));
        }
        return score;
    }

    private BigDecimal relativeDiff(BigDecimal value, BigDecimal ref) {
        if (value.signum() <= 0 || ref.signum() <= 0) return BigDecimal.valueOf(1000);
        return value.subtract(ref).abs().divide(ref, 18, RoundingMode.HALF_UP);
    }

    private BigDecimal firstValue(JsonNode levels, int idx) {
        if (levels == null || !levels.isArray()) return BigDecimal.ZERO;
        for (JsonNode level : levels) {
            if (!level.isArray() || level.size() < 2) continue;
            BigDecimal v = dec(level.get(idx));
            if (v.signum() > 0) return v;
        }
        return BigDecimal.ZERO;
    }

    private JsonNode arr(JsonNode d, String a, String b) { return d.has(a) ? d.get(a) : d.get(b); }
    private BigDecimal ticker(String symbol) { JsonNode d = requireOk(publicGet("/openApi/spot/v1/ticker/price", Map.of("symbol", symbol)), "ticker price"); return d.isArray() && d.size() > 0 ? dec(d.get(0).get("price")) : dec(d.get("price")); }
    private BigDecimal step(BigDecimal q, BigDecimal s) { if (s == null || s.signum() <= 0) return q; return s.multiply(q.divide(s, 0, RoundingMode.DOWN)).stripTrailingZeros(); }
    private String uri(String p, Map<String, String> m) { return m == null || m.isEmpty() ? p : p + "?" + qs(m); }
    private String signUri(String p, Map<String, String> m) { Map<String, String> all = new LinkedHashMap<>(); if (m != null) all.putAll(m); all.put("timestamp", String.valueOf(System.currentTimeMillis())); all.put("recvWindow", "5000"); String q = qs(new TreeMap<>(all)); return p + "?" + q + "&signature=" + sig(secret(), q); }
    private String qs(Map<String, String> m) { StringBuilder sb = new StringBuilder(); for (Map.Entry<String, String> e : m.entrySet()) if (e.getValue() != null) { if (!sb.isEmpty()) sb.append('&'); sb.append(enc(e.getKey())).append('=').append(enc(e.getValue())); } return sb.toString(); }
    private String enc(String v) { try { return URLEncoder.encode(v, StandardCharsets.UTF_8.toString()).replace("+", "%20"); } catch (Exception e) { throw new ExchangeException("Failed to encode BingX query", e); } }
    private String sig(String s, String payload) { try { Mac m = Mac.getInstance("HmacSHA256"); m.init(new SecretKeySpec(s.getBytes(StandardCharsets.UTF_8), "HmacSHA256")); byte[] b = m.doFinal(payload.getBytes(StandardCharsets.UTF_8)); StringBuilder h = new StringBuilder(); for (byte x : b) h.append(String.format("%02x", x)); return h.toString(); } catch (Exception e) { throw new ExchangeException("Failed to sign BingX request", e); } }
    private JsonNode json(String body, String ctx) { try { return MAPPER.readTree(StringUtils.defaultIfBlank(body, "{}")); } catch (Exception e) { throw new ExchangeException("Failed to parse BingX " + ctx + " response", e); } }
    private String key() { String k = secrets == null ? null : secrets.getApiKey(); if (StringUtils.isBlank(k)) throw new ExchangeException("Missing API credentials for bingx"); return k; }
    private String secret() { String s = secrets == null ? null : secrets.getApiSecret(); if (StringUtils.isBlank(s)) throw new ExchangeException("Missing API credentials for bingx"); return s; }
    private String[] split(String symbol) { if (symbol.contains("-")) return symbol.split("-"); if (symbol.contains("_")) return symbol.split("_"); String u = symbol.toUpperCase(); for (String q : new String[]{"USDT", "USDC", "BTC", "ETH", "USD", "EUR"}) if (u.endsWith(q) && u.length() > q.length()) return new String[]{u.substring(0, u.length() - q.length()), q}; return null; }
    private String textOf(JsonNode n, String... ks) { if (n == null) return null; for (String k : ks) if (n.hasNonNull(k)) { String v = n.get(k).asText(); if (StringUtils.isNotBlank(v)) return v; } return null; }
    private boolean bool(JsonNode n, String... ks) { if (n == null) return false; for (String k : ks) if (n.has(k)) return n.get(k).asBoolean(false); return false; }
    private BigDecimal dec(JsonNode n) { try { return n == null || n.isNull() ? BigDecimal.ZERO : new BigDecimal(n.asText("0")); } catch (Exception ignore) { return BigDecimal.ZERO; } }
    private long lng(JsonNode n, String... ks) { if (n == null) return 0L; for (String k : ks) if (n.has(k)) return n.get(k).asLong(0L); return 0L; }
    private String norm(String network) { if (StringUtils.isBlank(network)) return null; String c = network; int o = c.indexOf('('), z = c.indexOf(')'); if (o >= 0 && z > o) c = c.substring(o + 1, z); String x = c.replaceAll("[^A-Za-z0-9]", "").toUpperCase(); return switch (x) { case "ARBITRUMONE", "ARB" -> "ARBITRUM"; case "AVALANCHECCHAIN", "AVAXC", "AVAXCCHAIN" -> "AVAXC"; case "BNBSMARTCHAIN", "BSC", "BEP20" -> "BSC"; case "ETHEREUM", "ETH", "ERC20" -> "ERC20"; case "TRON", "TRX", "TRC20" -> "TRC20"; case "POLYGON", "MATIC" -> "MATIC"; case "SOLANA", "SOL" -> "SOL"; case "OPTIMISM", "OP" -> "OPTIMISM"; default -> x; }; }

    private record SymbolMeta(String symbol, BigDecimal step, BigDecimal minNotional) { }
    private record NetMeta(String name, boolean deposit, boolean withdraw) { }
    private record AssetMeta(String coin, List<NetMeta> networks) { }
}
