package com.crypto.console.exchanges.okx;

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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
public class OkxClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter OKX_TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    public OkxClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("okx", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        JsonNode result = requireOk(signedGet("/api/v5/account/balance", Map.of("ccy", asset.toUpperCase())), "account balance");
        JsonNode list = result == null ? null : result.get("data");
        if (list != null && list.isArray()) {
            for (JsonNode account : list) {
                JsonNode details = account.get("details");
                if (details == null || !details.isArray()) {
                    continue;
                }
                for (JsonNode row : details) {
                    String ccy = textOf(row, "ccy");
                    if (!asset.equalsIgnoreCase(ccy)) {
                        continue;
                    }
                    BigDecimal free = dec(row.get("availBal"));
                    BigDecimal frozen = dec(row.get("frozenBal"));
                    return new Balance(StringUtils.upperCase(ccy), free, frozen);
                }
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /api/v5/asset/currencies and parse minFee/maxFee");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolMeta symbol = resolveSymbol(base, quote);
        int sz = depth <= 0 ? 100 : Math.min(depth, 400);
        JsonNode result = requireOk(publicGet("/api/v5/market/books", Map.of("instId", symbol.instId, "sz", String.valueOf(sz))), "orderbook");
        JsonNode list = result.get("data");
        if (list == null || !list.isArray() || list.isEmpty()) {
            throw new ExchangeException("Unexpected response from OKX books API");
        }
        JsonNode book = list.get(0);
        return new OrderBook(symbol.instId, parseSide(book.get("bids")), parseSide(book.get("asks")));
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode result = requireOk(publicGet("/api/v5/market/books", Map.of("instId", symbol.instId, "sz", "400")), "orderbook");
        JsonNode list = result.get("data");
        if (list == null || !list.isArray() || list.isEmpty()) {
            throw new ExchangeException("Unexpected response from OKX books API");
        }
        return impact(symbol.instId, quoteAmount, list.get(0).get("asks"), true);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode result = requireOk(publicGet("/api/v5/market/books", Map.of("instId", symbol.instId, "sz", "400")), "orderbook");
        JsonNode list = result.get("data");
        if (list == null || !list.isArray() || list.isEmpty()) {
            throw new ExchangeException("Unexpected response from OKX books API");
        }
        return impact(symbol.instId, quoteAmount, list.get(0).get("bids"), false);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        if (symbol.minNotional.signum() > 0 && quoteAmount.compareTo(symbol.minNotional) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min notional " + symbol.minNotional + " for " + symbol.instId);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instId", symbol.instId);
        body.put("tdMode", "cash");
        body.put("side", "buy");
        body.put("ordType", "market");
        body.put("tgtCcy", "quote_ccy");
        body.put("sz", quoteAmount.toPlainString());
        body.put("clOrdId", "cc-" + UUID.randomUUID());
        JsonNode result = requireOk(signedPost("/api/v5/trade/order", body), "trade order");
        JsonNode data = result.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new ExchangeException("Unexpected response from OKX order API");
        }
        JsonNode row = data.get(0);
        String ordId = textOf(row, "ordId", "clOrdId");
        if (StringUtils.isBlank(ordId)) {
            throw new ExchangeException("Missing order id from OKX order API");
        }
        return new OrderResult(ordId, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        BigDecimal qty = applyStep(baseAmount, symbol.qtyStep);
        if (symbol.minQty.signum() > 0 && qty.compareTo(symbol.minQty) < 0) {
            throw new ExchangeException("Sell quantity " + qty + " below min quantity " + symbol.minQty + " for " + symbol.instId);
        }
        if (symbol.minNotional.signum() > 0) {
            BigDecimal notional = qty.multiply(lastPrice(symbol.instId));
            if (notional.compareTo(symbol.minNotional) < 0) {
                throw new ExchangeException("Order value " + notional + " below min notional " + symbol.minNotional + " for " + symbol.instId);
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instId", symbol.instId);
        body.put("tdMode", "cash");
        body.put("side", "sell");
        body.put("ordType", "market");
        body.put("tgtCcy", "base_ccy");
        body.put("sz", qty.toPlainString());
        body.put("clOrdId", "cc-" + UUID.randomUUID());
        JsonNode result = requireOk(signedPost("/api/v5/trade/order", body), "trade order");
        JsonNode data = result.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new ExchangeException("Unexpected response from OKX order API");
        }
        JsonNode row = data.get(0);
        String ordId = textOf(row, "ordId", "clOrdId");
        if (StringUtils.isBlank(ordId)) {
            throw new ExchangeException("Missing order id from OKX order API");
        }
        return new OrderResult(ordId, "SUBMITTED", "market sell submitted");
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
        CurrencyMeta currencyMeta = resolveCurrency(asset);
        ChainMeta chain = selectChain(currencyMeta, network, true);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ccy", currencyMeta.ccy);
        body.put("amt", amount.toPlainString());
        body.put("dest", "4");
        body.put("toAddr", address);
        body.put("chain", chain.chain);
        body.put("fee", chain.minFee.signum() > 0 ? chain.minFee.toPlainString() : "0");
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("addrTag", memoOrNull);
        }
        JsonNode result = requireOk(signedPost("/api/v5/asset/withdrawal", body), "withdrawal");
        JsonNode data = result.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new ExchangeException("Unexpected response from OKX withdrawal API");
        }
        String wdId = textOf(data.get(0), "wdId");
        if (StringUtils.isBlank(wdId)) {
            throw new ExchangeException("Missing withdrawal id from OKX withdrawal API");
        }
        return new WithdrawResult(wdId, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public String getWithdrawStatus(String asset) {
        CurrencyMeta currencyMeta = resolveCurrency(asset);
        if (currencyMeta.chains == null || currencyMeta.chains.isEmpty()) {
            return "withdraw status: unavailable";
        }
        List<String> statuses = new ArrayList<>();
        for (ChainMeta chain : currencyMeta.chains) {
            if (StringUtils.isBlank(chain.chain)) {
                continue;
            }
            statuses.add(StringUtils.upperCase(chain.chain) + "=" + (chain.canWd ? "enabled" : "disabled"));
        }
        if (statuses.isEmpty()) {
            return "withdraw status: unavailable";
        }
        return "withdraw status: " + String.join(", ", statuses);
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode result = requireOk(publicGet("/api/v5/public/time", Map.of()), "server time");
        JsonNode data = result.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new ExchangeException("Unexpected response from OKX time API");
        }
        long ts = data.get(0).path("ts").asLong(System.currentTimeMillis());
        return new ExchangeTime(ts, ts - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        CurrencyMeta currencyMeta = resolveCurrency(asset);
        Set<String> networks = new LinkedHashSet<>();
        for (ChainMeta chain : currencyMeta.chains) {
            if (chain.canDep) {
                networks.add(StringUtils.upperCase(chain.chain));
            }
        }
        if (networks.isEmpty()) {
            throw new ExchangeException("OKX does not support deposits for asset: " + asset.toUpperCase());
        }
        return networks;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        CurrencyMeta currencyMeta = resolveCurrency(asset);
        ChainMeta chain = selectChain(currencyMeta, network, false);
        JsonNode result = requireOk(signedGet("/api/v5/asset/deposit-address", Map.of("ccy", currencyMeta.ccy)), "deposit address");
        JsonNode data = result.get("data");
        if (data != null && data.isArray()) {
            String wanted = normalizeDepositNetwork(chain.chain);
            for (JsonNode row : data) {
                String rowChain = normalizeDepositNetwork(textOf(row, "chain"));
                if (StringUtils.isBlank(rowChain)) {
                    continue;
                }
                if (!wanted.equals(rowChain) && !(rowChain.contains(wanted) || wanted.contains(rowChain))) {
                    continue;
                }
                String addr = textOf(row, "addr");
                if (StringUtils.isNotBlank(addr)) {
                    return addr;
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
            case "TRX", "TRON", "TRC20" -> "TRC20";
            case "BSC", "BEP20", "BNBSMARTCHAIN" -> "BSC";
            case "ARBITRUM", "ARBITRUMONE", "ARB" -> "ARBITRUM";
            case "POLYGON", "MATIC" -> "POLYGON";
            case "AVAXC", "AVALANCHECCHAIN" -> "AVAXC";
            case "SOL", "SOLANA" -> "SOL";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            default -> n;
        };
    }

    private SymbolMeta resolveSymbol(String base, String quote) {
        JsonNode result = requireOk(publicGet("/api/v5/public/instruments", Map.of("instType", "SPOT")), "instruments");
        JsonNode data = result.get("data");
        if (data == null || !data.isArray()) {
            throw new ExchangeException("Unexpected response from OKX instruments API");
        }
        String baseUpper = base.toUpperCase();
        String quoteUpper = quote.toUpperCase();
        for (JsonNode row : data) {
            String baseCcy = textOf(row, "baseCcy");
            String quoteCcy = textOf(row, "quoteCcy");
            String state = textOf(row, "state");
            if (StringUtils.isNotBlank(state) && !"live".equalsIgnoreCase(state)) {
                continue;
            }
            if (!baseUpper.equalsIgnoreCase(baseCcy) || !quoteUpper.equalsIgnoreCase(quoteCcy)) {
                continue;
            }
            String instId = textOf(row, "instId");
            BigDecimal minQty = dec(row.get("minSz"));
            BigDecimal qtyStep = dec(row.get("lotSz"));
            BigDecimal minNotional = dec(row.get("minNotional"));
            if (minNotional.signum() <= 0) {
                minNotional = dec(row.get("minAmt"));
            }
            return new SymbolMeta(instId, minQty, qtyStep, minNotional);
        }
        throw new ExchangeException("Invalid symbol: " + baseUpper + "/" + quoteUpper);
    }

    private CurrencyMeta resolveCurrency(String asset) {
        JsonNode result = requireOk(signedGet("/api/v5/asset/currencies", Map.of("ccy", asset.toUpperCase())), "currencies");
        JsonNode data = result.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new ExchangeException("OKX does not support asset: " + asset.toUpperCase());
        }
        String ccy = asset.toUpperCase();
        List<ChainMeta> chains = new ArrayList<>();
        for (JsonNode row : data) {
            String rowCcy = textOf(row, "ccy");
            if (StringUtils.isNotBlank(rowCcy)) {
                ccy = rowCcy.toUpperCase();
            }
            String chain = textOf(row, "chain");
            if (StringUtils.isBlank(chain)) {
                continue;
            }
            boolean canDep = "true".equalsIgnoreCase(textOf(row, "canDep"));
            boolean canWd = "true".equalsIgnoreCase(textOf(row, "canWd"));
            BigDecimal minFee = dec(row.get("minFee"));
            // OKX expects the exact chain id returned by currencies API (case-sensitive).
            chains.add(new ChainMeta(chain, canDep, canWd, minFee));
        }
        return new CurrencyMeta(ccy, chains);
    }

    private ChainMeta selectChain(CurrencyMeta meta, String requested, boolean withdraw) {
        List<ChainMeta> candidates = new ArrayList<>();
        for (ChainMeta chain : meta.chains) {
            if (withdraw ? chain.canWd : chain.canDep) {
                candidates.add(chain);
            }
        }
        if (candidates.isEmpty() && !meta.chains.isEmpty()) {
            candidates.addAll(meta.chains);
        }
        if (candidates.isEmpty()) {
            throw new ExchangeException("No " + (withdraw ? "withdraw" : "deposit") + " networks for asset " + meta.ccy);
        }
        String req = normalizeDepositNetwork(requested);
        if (StringUtils.isBlank(req)) {
            if (candidates.size() == 1) {
                return candidates.get(0);
            }
            StringBuilder available = new StringBuilder();
            for (ChainMeta chain : candidates) {
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(chain.chain);
            }
            throw new ExchangeException("Network is required for " + meta.ccy + " (available: " + available + ")");
        }
        for (ChainMeta chain : candidates) {
            String normalized = normalizeDepositNetwork(chain.chain);
            if (req.equals(normalized) || (normalized != null && (normalized.contains(req) || req.contains(normalized)))) {
                return chain;
            }
        }
        throw new ExchangeException("OKX does not support network " + requested + " for asset " + meta.ccy);
    }

    private BigDecimal lastPrice(String instId) {
        JsonNode result = requireOk(publicGet("/api/v5/market/ticker", Map.of("instId", instId)), "ticker");
        JsonNode data = result.get("data");
        if (data != null && data.isArray() && !data.isEmpty()) {
            JsonNode row = data.get(0);
            BigDecimal ask = dec(row.get("askPx"));
            if (ask.signum() > 0) {
                return ask;
            }
            return dec(row.get("last"));
        }
        return BigDecimal.ZERO;
    }

    private BuyInfoResult impact(String symbol, BigDecimal quoteAmount, JsonNode levels, boolean buy) {
        if (levels == null || !levels.isArray()) {
            throw new ExchangeException("Unexpected response from OKX orderbook API");
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

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = path;
        if (params != null && !params.isEmpty()) {
            uri = uri + "?" + qs(params);
        }
        LOG.info("okx GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("OKX request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "public");
    }

    private JsonNode signedGet(String path, Map<String, String> params) {
        Map<String, String> sorted = new TreeMap<>();
        if (params != null) {
            sorted.putAll(params);
        }
        String query = qs(sorted);
        String requestPath = query.isEmpty() ? path : path + "?" + query;
        Auth auth = sign("GET", requestPath, "");
        LOG.info("okx GET {}", LogSanitizer.sanitize(requestPath));
        String body = webClient.get()
                .uri(requestPath)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("OK-ACCESS-KEY", apiKey())
                .header("OK-ACCESS-SIGN", auth.sign)
                .header("OK-ACCESS-TIMESTAMP", auth.ts)
                .header("OK-ACCESS-PASSPHRASE", passphrase())
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("OKX request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "signed GET");
    }

    private JsonNode signedPost(String path, Map<String, Object> payload) {
        String bodyText = toJson(payload);
        Auth auth = sign("POST", path, bodyText);
        LOG.info("okx POST {}", LogSanitizer.sanitize(path + "?" + bodyText));
        String body = webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header("OK-ACCESS-KEY", apiKey())
                .header("OK-ACCESS-SIGN", auth.sign)
                .header("OK-ACCESS-TIMESTAMP", auth.ts)
                .header("OK-ACCESS-PASSPHRASE", passphrase())
                .bodyValue(bodyText)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("OKX request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "signed POST");
    }

    private JsonNode requireOk(JsonNode root, String context) {
        if (root == null || root.isNull()) {
            throw new ExchangeException("Unexpected response from OKX " + context + " API");
        }
        String code = textOf(root, "code");
        if (StringUtils.isNotBlank(code) && !"0".equals(code)) {
            String msg = textOf(root, "msg");
            throw new ExchangeException("OKX " + context + " failed: code=" + code + " " + StringUtils.defaultString(msg));
        }
        return root;
    }

    private Auth sign(String method, String requestPath, String body) {
        String ts = OKX_TS_FMT.format(Instant.now());
        String prehash = ts + method.toUpperCase() + requestPath + StringUtils.defaultString(body);
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(apiSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            String sign = Base64.getEncoder().encodeToString(mac.doFinal(prehash.getBytes(StandardCharsets.UTF_8)));
            return new Auth(ts, sign);
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign OKX request", e);
        }
    }

    private String qs(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (!sb.isEmpty()) {
                sb.append("&");
            }
            sb.append(enc(entry.getKey())).append("=").append(enc(entry.getValue()));
        }
        return sb.toString();
    }

    private String enc(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString()).replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode OKX query parameter", e);
        }
    }

    private JsonNode readJson(String body, String context) {
        try {
            return MAPPER.readTree(StringUtils.defaultIfBlank(body, "{}"));
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse OKX " + context + " response", e);
        }
    }

    private String toJson(Object payload) {
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize OKX request body", e);
        }
    }

    private String apiKey() {
        String value = secrets == null ? null : secrets.getApiKey();
        if (StringUtils.isBlank(value)) {
            throw new ExchangeException("Missing API credentials for okx");
        }
        return value;
    }

    private String apiSecret() {
        String value = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(value)) {
            throw new ExchangeException("Missing API credentials for okx");
        }
        return value;
    }

    private String passphrase() {
        String value = secrets == null ? null : secrets.getApiPassphrase();
        if (StringUtils.isBlank(value)) {
            throw new ExchangeException("Missing API passphrase for okx");
        }
        return value;
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

    private record Auth(String ts, String sign) {
    }

    private record SymbolMeta(String instId, BigDecimal minQty, BigDecimal qtyStep, BigDecimal minNotional) {
    }

    private record ChainMeta(String chain, boolean canDep, boolean canWd, BigDecimal minFee) {
    }

    private record CurrencyMeta(String ccy, List<ChainMeta> chains) {
    }
}
