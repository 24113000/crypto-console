package com.crypto.console.exchanges.poloniex;

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
import org.springframework.http.MediaType;

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

@Slf4j
public class PoloniexClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public PoloniexClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("poloniex", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        JsonNode list = privateGet("/accounts/balances", Map.of("accountType", "SPOT"));
        if (list == null || !list.isArray()) {
            throw new ExchangeException("Unexpected response from Poloniex balances API");
        }
        for (JsonNode account : list) {
            JsonNode balances = account.get("balances");
            if (balances == null || !balances.isArray()) {
                continue;
            }
            for (JsonNode b : balances) {
                String c = textOf(b, "currency");
                if (asset.equalsIgnoreCase(c)) {
                    return new Balance(c.toUpperCase(), dec(b.get("available")), dec(b.get("hold")));
                }
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /v2/currencies/{currency} for network fees");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolMeta s = resolveSymbol(base, quote);
        int limit = depth <= 0 ? 50 : Math.min(depth, 150);
        JsonNode ob = publicGet("/markets/" + s.symbol + "/orderBook", Map.of("limit", String.valueOf(limit)));
        return new OrderBook(s.symbol, parseFlatOrderSide(ob.get("bids")), parseFlatOrderSide(ob.get("asks")));
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        JsonNode ob = publicGet("/markets/" + s.symbol + "/orderBook", Map.of("limit", "150"));
        return bookImpact(s.symbol, quoteAmount, ob == null ? null : ob.get("asks"), true);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta s = resolveSymbol(base, quote);
        JsonNode ob = publicGet("/markets/" + s.symbol + "/orderBook", Map.of("limit", "150"));
        return bookImpact(s.symbol, quoteAmount, ob == null ? null : ob.get("bids"), false);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        SymbolMeta s = resolveSymbol(base, quote);
        if (s.minAmount.signum() > 0 && quoteAmount.compareTo(s.minAmount) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min amount " + s.minAmount + " for " + s.symbol);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("symbol", s.symbol);
        body.put("side", "BUY");
        body.put("type", "MARKET");
        body.put("amount", quoteAmount.toPlainString());
        JsonNode r = privatePost("/orders", body);
        return new OrderResult(textOf(r, "id", "clientOrderId"), "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        SymbolMeta s = resolveSymbol(base, quote);
        BigDecimal qty = scaleDown(baseAmount, s.quantityScale);
        if (s.minQuantity.signum() > 0 && qty.compareTo(s.minQuantity) < 0) {
            throw new ExchangeException("Sell quantity " + qty + " below min quantity " + s.minQuantity + " for " + s.symbol);
        }
        if (s.minAmount.signum() > 0) {
            BigDecimal p = priceOf(s.symbol);
            if (p.signum() > 0 && qty.multiply(p).compareTo(s.minAmount) < 0) {
                throw new ExchangeException("Order value below min amount " + s.minAmount + " for " + s.symbol);
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("symbol", s.symbol);
        body.put("side", "SELL");
        body.put("type", "MARKET");
        body.put("quantity", qty.toPlainString());
        JsonNode r = privatePost("/orders", body);
        return new OrderResult(textOf(r, "id", "clientOrderId"), "SUBMITTED", "market sell submitted");
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        CurrencyV2 c = resolveCurrency(asset);
        NetworkV2 n = selectNetwork(c, network, true);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("coin", c.coin);
        body.put("network", n.blockchain);
        body.put("amount", amount.toPlainString());
        body.put("address", address);
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("addressTag", memoOrNull);
        }
        JsonNode r = privatePost("/v2/wallets/withdraw", body);
        String id = textOf(r, "withdrawalRequestsId", "id");
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from Poloniex withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public String getWithdrawStatus(String asset) {
        CurrencyV2 c = resolveCurrency(asset);
        if (c.networks == null || c.networks.isEmpty()) {
            return "withdraw status: unavailable";
        }
        List<String> statuses = new ArrayList<>();
        for (NetworkV2 n : c.networks) {
            if (StringUtils.isBlank(n.blockchain)) {
                continue;
            }
            statuses.add(StringUtils.upperCase(n.blockchain) + "=" + (n.withdrawalEnabled ? "enabled" : "disabled"));
        }
        if (statuses.isEmpty()) {
            return "withdraw status: unavailable";
        }
        return "withdraw status: " + String.join(", ", statuses);
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode t = publicGet("/timestamp", Map.of());
        long server = t.isNumber() ? t.asLong() : t.asLong(System.currentTimeMillis());
        return new ExchangeTime(server, server - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        CurrencyV2 c = resolveCurrency(asset);
        Set<String> out = new LinkedHashSet<>();
        for (NetworkV2 n : c.networks) {
            if (n.depositEnabled) out.add(StringUtils.upperCase(n.blockchain));
        }
        if (out.isEmpty()) throw new ExchangeException("Poloniex does not support deposits for asset: " + asset.toUpperCase());
        return out;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        CurrencyV2 c = resolveCurrency(asset);
        NetworkV2 n = selectNetwork(c, network, false);
        String currencyKey = StringUtils.upperCase(n.coin);
        JsonNode map = privateGet("/wallets/addresses", Map.of("currency", currencyKey));
        String addr = extractAddress(map, currencyKey, n.blockchain);
        if (StringUtils.isBlank(addr)) {
            JsonNode created = privatePost("/wallets/address", Map.of("currency", currencyKey));
            addr = extractAddress(created, currencyKey, n.blockchain);
        }
        if (StringUtils.isBlank(addr)) throw new ExchangeException("No deposit address returned for " + asset + " " + network);
        return addr;
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, true, true, true, true, true);
    }

    @Override
    public String normalizeDepositNetwork(String network) {
        if (StringUtils.isBlank(network)) return null;
        String c = network;
        int o = c.indexOf('('), z = c.indexOf(')');
        if (o >= 0 && z > o) c = c.substring(o + 1, z);
        String n = c.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        return switch (n) {
            case "ARBITRUMONE", "ARB" -> "ARBITRUM";
            case "AVALANCHECCHAIN", "AVAXC", "AVAXCCHAIN" -> "AVAXC";
            case "BNBSMARTCHAIN", "BSC", "BEP20" -> "BSC";
            case "ETHEREUM", "ETH", "ERC20" -> "ETH";
            case "TRON", "TRX", "TRC20" -> "TRX";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL" -> "SOL";
            default -> n;
        };
    }

    private SymbolMeta resolveSymbol(String base, String quote) {
        JsonNode markets = publicGet("/markets", Map.of());
        if (markets == null || !markets.isArray()) {
            throw new ExchangeException("Unexpected response from Poloniex markets API");
        }
        for (JsonNode m : markets) {
            String b = textOf(m, "baseCurrencyName", "baseCurrency");
            String q = textOf(m, "quoteCurrencyName", "quoteCurrency");
            if (base.equalsIgnoreCase(b) && quote.equalsIgnoreCase(q)) {
                String symbol = textOf(m, "symbol");
                JsonNode lim = m.get("symbolTradeLimit");
                return new SymbolMeta(symbol, lim == null ? 8 : lim.path("quantityScale").asInt(8), dec(lim == null ? null : lim.get("minQuantity")), dec(lim == null ? null : lim.get("minAmount")));
            }
        }
        throw new ExchangeException("Invalid symbol: " + base.toUpperCase() + "/" + quote.toUpperCase());
    }

    private CurrencyV2 resolveCurrency(String asset) {
        JsonNode c = publicGet("/v2/currencies/" + asset.toUpperCase(), Map.of());
        String coin = textOf(c, "coin");
        if (StringUtils.isBlank(coin)) throw new ExchangeException("Poloniex does not support asset: " + asset.toUpperCase());
        List<NetworkV2> nets = new ArrayList<>();
        JsonNode list = c.get("networkList");
        if (list != null && list.isArray()) {
            for (JsonNode n : list) {
                nets.add(new NetworkV2(
                        StringUtils.upperCase(textOf(n, "coin")),
                        StringUtils.upperCase(textOf(n, "blockchain")),
                        boolWithDefault(n, true, "depositEnable", "depositEnabled", "deposit", "depositStatus"),
                        boolWithDefault(n, true, "withdrawEnable", "withdrawalEnable", "withdrawEnabled", "withdraw", "withdrawStatus")
                ));
            }
        }
        return new CurrencyV2(coin.toUpperCase(), nets);
    }

    private NetworkV2 selectNetwork(CurrencyV2 c, String requested, boolean withdraw) {
        List<NetworkV2> candidates = new ArrayList<>();
        for (NetworkV2 n : c.networks) if (withdraw ? n.withdrawalEnabled : n.depositEnabled) candidates.add(n);
        if (candidates.isEmpty() && !c.networks.isEmpty()) {
            candidates.addAll(c.networks);
        }
        if (candidates.isEmpty()) throw new ExchangeException("No " + (withdraw ? "withdraw" : "deposit") + " networks for asset " + c.coin);
        String req = normalizeDepositNetwork(requested);
        if (StringUtils.isBlank(req)) {
            if (candidates.size() == 1) return candidates.get(0);
            StringBuilder sb = new StringBuilder();
            for (NetworkV2 n : candidates) { if (!sb.isEmpty()) sb.append(", "); sb.append(n.blockchain); }
            throw new ExchangeException("Network is required for " + c.coin + " (available: " + sb + ")");
        }
        for (NetworkV2 n : candidates) {
            String nn = normalizeDepositNetwork(n.blockchain);
            String nc = normalizeDepositNetwork(n.coin);
            if (req.equals(nn) || req.equals(nc)) return n;
            if (StringUtils.isNotBlank(nn) && (nn.contains(req) || req.contains(nn))) return n;
            if (StringUtils.isNotBlank(nc) && (nc.contains(req) || req.contains(nc))) return n;
        }
        throw new ExchangeException("Poloniex does not support network " + requested + " for asset " + c.coin);
    }

    private BuyInfoResult bookImpact(String symbol, BigDecimal quoteAmount, JsonNode flat, boolean buy) {
        if (flat == null || !flat.isArray()) throw new ExchangeException("Unexpected response from Poloniex order book API");
        BigDecimal rem = quoteAmount, quote = BigDecimal.ZERO, base = BigDecimal.ZERO;
        List<BuyInfoItem> items = new ArrayList<>();
        for (int i = 0; i < flat.size(); i++) {
            JsonNode level = flat.get(i);
            BigDecimal p;
            BigDecimal q;
            if (level != null && level.isArray() && level.size() >= 2) {
                p = dec(level.get(0));
                q = dec(level.get(1));
            } else if (i + 1 < flat.size()) {
                p = dec(flat.get(i));
                q = dec(flat.get(i + 1));
                i++;
            } else {
                continue;
            }
            if (p.signum() <= 0 || q.signum() <= 0) continue;
            BigDecimal v = p.multiply(q);
            if (rem.compareTo(v) >= 0) {
                base = base.add(q); quote = quote.add(v); rem = rem.subtract(v); items.add(new BuyInfoItem(p, q, v));
            } else {
                BigDecimal part = rem.divide(p, 18, RoundingMode.DOWN);
                if (part.signum() > 0) { BigDecimal pv = part.multiply(p); base = base.add(part); quote = quote.add(pv); items.add(new BuyInfoItem(p, part, pv)); }
                rem = BigDecimal.ZERO; break;
            }
        }
        if (base.signum() <= 0) throw new ExchangeException("No " + (buy ? "ask" : "bid") + " liquidity available for " + symbol);
        return new BuyInfoResult(symbol, quoteAmount, quote, base, quote.divide(base, 18, RoundingMode.HALF_UP), List.copyOf(items));
    }

    private List<OrderBookEntry> parseFlatOrderSide(JsonNode flat) {
        List<OrderBookEntry> out = new ArrayList<>();
        if (flat == null || !flat.isArray()) return out;
        for (int i = 0; i < flat.size(); i++) {
            JsonNode level = flat.get(i);
            BigDecimal p;
            BigDecimal q;
            if (level != null && level.isArray() && level.size() >= 2) {
                p = dec(level.get(0));
                q = dec(level.get(1));
            } else if (i + 1 < flat.size()) {
                p = dec(flat.get(i));
                q = dec(flat.get(i + 1));
                i++;
            } else {
                continue;
            }
            if (p.signum() > 0 && q.signum() > 0) out.add(new OrderBookEntry(p, q));
        }
        return out;
    }

    private BigDecimal priceOf(String symbol) {
        JsonNode p = publicGet("/markets/" + symbol + "/price", Map.of());
        return dec(p.get("price"));
    }

    private String extractAddress(JsonNode node, String currencyKey, String blockchain) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return StringUtils.trimToNull(node.asText());
        }
        String direct = textOf(node, "address");
        if (StringUtils.isNotBlank(direct)) {
            return direct;
        }
        JsonNode exact = node.get(currencyKey);
        if (exact != null) {
            if (exact.isTextual()) {
                return StringUtils.trimToNull(exact.asText());
            }
            String nested = textOf(exact, "address");
            if (StringUtils.isNotBlank(nested)) {
                return nested;
            }
        }
        if (node.isObject()) {
            String wantedNetwork = normalizeDepositNetwork(blockchain);
            for (java.util.Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                String key = StringUtils.upperCase(entry.getKey());
                if (StringUtils.isBlank(key)) {
                    continue;
                }
                if (!key.startsWith(currencyKey)) {
                    continue;
                }
                if (StringUtils.isNotBlank(wantedNetwork)
                        && !normalizeDepositNetwork(key).contains(wantedNetwork)) {
                    continue;
                }
                JsonNode value = entry.getValue();
                if (value == null || value.isNull()) {
                    continue;
                }
                if (value.isTextual()) {
                    String v = StringUtils.trimToNull(value.asText());
                    if (StringUtils.isNotBlank(v)) {
                        return v;
                    }
                }
                String nested = textOf(value, "address");
                if (StringUtils.isNotBlank(nested)) {
                    return nested;
                }
            }
        }
        return null;
    }

    private BigDecimal scaleDown(BigDecimal v, int scale) {
        if (v == null) return BigDecimal.ZERO;
        return v.setScale(Math.max(scale, 0), RoundingMode.DOWN).stripTrailingZeros();
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = params == null || params.isEmpty() ? path : path + "?" + qs(params);
        LOG.info("poloniex GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get().uri(uri).header(HttpHeaders.USER_AGENT, "crypto-console").retrieve().bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> reactor.core.publisher.Mono.error(new ExchangeException("Poloniex request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readAndCheck(body, "public");
    }

    private JsonNode privateGet(String path, Map<String, String> params) {
        String query = params == null || params.isEmpty() ? "" : qs(new TreeMap<>(params));
        String uri = query.isEmpty() ? path : path + "?" + query;
        Auth a = sign("GET", path, query, null);
        LOG.info("poloniex GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get().uri(uri).header(HttpHeaders.USER_AGENT, "crypto-console").header("key", key()).header("signatureMethod", "HmacSHA256").header("signatureVersion", "2").header("signTimestamp", a.ts).header("recvWindow", "5000").header("signature", a.sig).retrieve().bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> reactor.core.publisher.Mono.error(new ExchangeException("Poloniex request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readAndCheck(body, "private GET");
    }

    private JsonNode privatePost(String path, Map<String, Object> body) {
        String json = toJson(body);
        Auth a = sign("POST", path, "", json);
        LOG.info("poloniex POST {}", LogSanitizer.sanitize(path + "?requestBody=" + json));
        String resp = webClient.post().uri(path).header(HttpHeaders.USER_AGENT, "crypto-console").header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE).header("key", key()).header("signatureMethod", "HmacSHA256").header("signatureVersion", "2").header("signTimestamp", a.ts).header("recvWindow", "5000").header("signature", a.sig).bodyValue(json).retrieve().bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> reactor.core.publisher.Mono.error(new ExchangeException("Poloniex request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readAndCheck(resp, "private POST");
    }

    private String qs(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (e.getValue() == null) continue;
            if (!sb.isEmpty()) sb.append("&");
            sb.append(enc(e.getKey())).append("=").append(enc(e.getValue()));
        }
        return sb.toString();
    }

    private String enc(String v) {
        try { return URLEncoder.encode(v, StandardCharsets.UTF_8.toString()).replace("+", "%20"); }
        catch (Exception e) { throw new ExchangeException("Failed to encode Poloniex query", e); }
    }

    private Auth sign(String method, String path, String query, String body) {
        String ts = String.valueOf(System.currentTimeMillis());
        String payloadParams;
        if (StringUtils.isNotBlank(body)) {
            payloadParams = "requestBody=" + body + "&signTimestamp=" + ts;
        } else if (StringUtils.isNotBlank(query)) {
            payloadParams = query + "&signTimestamp=" + ts;
        } else {
            payloadParams = "signTimestamp=" + ts;
        }
        String payload = method.toUpperCase() + "\n" + path + "\n" + payloadParams;
        String sig = hmacBase64(secret(), payload);
        return new Auth(ts, sig);
    }

    private String hmacBase64(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return Base64.getEncoder().encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) { throw new ExchangeException("Failed to sign Poloniex request", e); }
    }

    private JsonNode readAndCheck(String body, String context) {
        try {
            JsonNode json = MAPPER.readTree(StringUtils.defaultIfBlank(body, "{}"));
            if (json != null && json.has("code") && json.has("message")) {
                String code = json.get("code").asText();
                if (!"0".equals(code) && !"200".equals(code)) throw new ExchangeException("Poloniex " + context + " failed: code=" + code + " " + json.get("message").asText());
            }
            if (json != null && json.has("error")) {
                throw new ExchangeException("Poloniex " + context + " failed: " + json.get("error").asText());
            }
            return json;
        } catch (ExchangeException e) {
            throw e;
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse Poloniex " + context + " response", e);
        }
    }

    private String key() { String k = secrets == null ? null : secrets.getApiKey(); if (StringUtils.isBlank(k)) throw new ExchangeException("Missing API credentials for poloniex"); return k; }
    private String secret() { String s = secrets == null ? null : secrets.getApiSecret(); if (StringUtils.isBlank(s)) throw new ExchangeException("Missing API credentials for poloniex"); return s; }
    private String textOf(JsonNode n, String... keys) { if (n == null) return null; for (String k : keys) if (n.hasNonNull(k)) { String v = n.get(k).asText(); if (StringUtils.isNotBlank(v)) return v; } return null; }
    private boolean boolOf(JsonNode n, String... keys) { if (n == null || keys == null) return false; for (String k : keys) if (n.has(k)) return n.get(k).asBoolean(false); return false; }
    private boolean boolWithDefault(JsonNode n, boolean defaultValue, String... keys) {
        if (n == null || keys == null || keys.length == 0) return defaultValue;
        boolean seen = false;
        for (String k : keys) {
            if (!n.has(k)) continue;
            seen = true;
            JsonNode v = n.get(k);
            if (v.isBoolean()) return v.asBoolean();
            String s = StringUtils.trimToEmpty(v.asText()).toLowerCase();
            if (s.isEmpty()) continue;
            if ("true".equals(s) || "1".equals(s) || "enabled".equals(s) || "allow".equals(s) || "allowed".equals(s) || "open".equals(s)) return true;
            if ("false".equals(s) || "0".equals(s) || "disabled".equals(s) || "deny".equals(s) || "denied".equals(s) || "close".equals(s) || "closed".equals(s)) return false;
        }
        return seen ? defaultValue : defaultValue;
    }
    private BigDecimal dec(JsonNode n) { try { return n == null || n.isNull() ? BigDecimal.ZERO : new BigDecimal(n.asText("0")); } catch (Exception ignore) { return BigDecimal.ZERO; } }
    private String toJson(Object o) { try { return MAPPER.writeValueAsString(o); } catch (Exception e) { throw new ExchangeException("Failed to serialize Poloniex request", e); } }

    private record Auth(String ts, String sig) {}
    private record SymbolMeta(String symbol, int quantityScale, BigDecimal minQuantity, BigDecimal minAmount) {}
    private record NetworkV2(String coin, String blockchain, boolean depositEnabled, boolean withdrawalEnabled) {}
    private record CurrencyV2(String coin, List<NetworkV2> networks) {}
}
