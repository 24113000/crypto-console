package com.crypto.console.exchanges.kucoin;

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
public class KuCoinClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public KuCoinClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("kucoin", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        JsonNode data = requireOk(signedGet("/api/v1/accounts", Map.of("currency", asset.toUpperCase(), "type", "trade")), "accounts").get("data");
        if (data == null || !data.isArray()) {
            throw new ExchangeException("Unexpected response from KuCoin accounts API");
        }
        for (JsonNode row : data) {
            String ccy = textOf(row, "currency");
            if (!asset.equalsIgnoreCase(ccy)) {
                continue;
            }
            return new Balance(StringUtils.upperCase(ccy), dec(row.get("available")), dec(row.get("holds")));
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /api/v3/currencies/{currency} and parse chains[].withdrawalMinFee");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode data = requireOk(publicGet("/api/v1/market/orderbook/level2_100", Map.of("symbol", symbol.symbol)), "orderbook").get("data");
        if (data == null || !data.isObject()) {
            throw new ExchangeException("Unexpected response from KuCoin orderbook API");
        }
        List<OrderBookEntry> bids = parseSide(data.get("bids"), depth);
        List<OrderBookEntry> asks = parseSide(data.get("asks"), depth);
        return new OrderBook(symbol.symbol, bids, asks);
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode data = requireOk(publicGet("/api/v1/market/orderbook/level2_100", Map.of("symbol", symbol.symbol)), "orderbook").get("data");
        return impact(symbol.symbol, quoteAmount, data == null ? null : data.get("asks"), true);
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        JsonNode data = requireOk(publicGet("/api/v1/market/orderbook/level2_100", Map.of("symbol", symbol.symbol)), "orderbook").get("data");
        return impact(symbol.symbol, quoteAmount, data == null ? null : data.get("bids"), false);
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        if (symbol.minFunds.signum() > 0 && quoteAmount.compareTo(symbol.minFunds) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min notional " + symbol.minFunds + " for " + symbol.symbol);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("clientOid", "cc-" + UUID.randomUUID());
        body.put("side", "buy");
        body.put("symbol", symbol.symbol);
        body.put("type", "market");
        body.put("funds", quoteAmount.toPlainString());
        JsonNode data = placeOrder(body);
        String orderId = textOf(data, "orderId");
        return new OrderResult(orderId, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        SymbolMeta symbol = resolveSymbol(base, quote);
        BigDecimal qty = applyStep(baseAmount, symbol.baseIncrement);
        if (symbol.baseMinSize.signum() > 0 && qty.compareTo(symbol.baseMinSize) < 0) {
            throw new ExchangeException("Sell quantity " + qty + " below min quantity " + symbol.baseMinSize + " for " + symbol.symbol);
        }
        if (symbol.minFunds.signum() > 0) {
            BigDecimal notional = qty.multiply(lastPrice(symbol.symbol));
            if (notional.compareTo(symbol.minFunds) < 0) {
                throw new ExchangeException("Order value " + notional + " below min notional " + symbol.minFunds + " for " + symbol.symbol);
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("clientOid", "cc-" + UUID.randomUUID());
        body.put("side", "sell");
        body.put("symbol", symbol.symbol);
        body.put("type", "market");
        body.put("size", qty.toPlainString());
        JsonNode data = placeOrder(body);
        String orderId = textOf(data, "orderId");
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
        CurrencyMeta currency = resolveCurrency(asset);
        ChainMeta chain = selectChain(currency, network, true);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("currency", currency.currency);
        body.put("address", address);
        body.put("amount", amount.toPlainString());
        body.put("chain", chain.chainName);
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("memo", memoOrNull);
        }
        JsonNode result;
        try {
            result = requireOk(signedPost("/api/v3/withdrawals", body), "withdrawal").get("data");
        } catch (ExchangeException ex) {
            result = requireOk(signedPost("/api/v2/withdrawals", body), "withdrawal").get("data");
        }
        String withdrawalId = textOf(result, "withdrawalId", "withdrawId");
        if (StringUtils.isBlank(withdrawalId)) {
            throw new ExchangeException("Missing withdrawal id from KuCoin withdrawal API");
        }
        return new WithdrawResult(withdrawalId, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode root = requireOk(publicGet("/api/v1/timestamp", Map.of()), "timestamp");
        long ts = root.path("data").asLong(System.currentTimeMillis());
        return new ExchangeTime(ts, ts - System.currentTimeMillis());
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        CurrencyMeta currency = resolveCurrency(asset);
        Set<String> out = new LinkedHashSet<>();
        for (ChainMeta chain : currency.chains) {
            if (chain.isDepositEnabled) {
                out.add(StringUtils.upperCase(chain.chainName));
            }
        }
        if (out.isEmpty()) {
            throw new ExchangeException("KuCoin does not support deposits for asset: " + asset.toUpperCase());
        }
        return out;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        CurrencyMeta currency = resolveCurrency(asset);
        ChainMeta chain = selectChain(currency, network, false);
        List<String> chainCandidates = new ArrayList<>();
        chainCandidates.add(chain.chainName);
        if (StringUtils.isNotBlank(chain.chainId) && !chain.chainId.equalsIgnoreCase(chain.chainName)) {
            chainCandidates.add(chain.chainId);
        }

        ExchangeException last = null;
        for (String chainCandidate : chainCandidates) {
            for (EndpointAttempt attempt : EndpointAttempt.values()) {
                try {
                    Map<String, String> params = Map.of("currency", currency.currency, "chain", chainCandidate);
                    JsonNode data = switch (attempt) {
                        case GET_V2 -> requireOk(signedGet("/api/v2/deposit-addresses", params), "deposit address").get("data");
                        case GET_V1 -> requireOk(signedGet("/api/v1/deposit-addresses", params), "deposit address").get("data");
                        case POST_V2 -> requireOk(signedPost("/api/v2/deposit-addresses", new LinkedHashMap<>(params)), "deposit address create").get("data");
                        case POST_V1 -> requireOk(signedPost("/api/v1/deposit-addresses", new LinkedHashMap<>(params)), "deposit address create").get("data");
                    };
                    String address = extractDepositAddress(data, chain);
                    if (StringUtils.isNotBlank(address)) {
                        return address;
                    }
                } catch (ExchangeException ex) {
                    last = ex;
                    if (isDepositDisabledError(ex)) {
                        throw new ExchangeException("KuCoin deposit is disabled for network " + network + " (" + chainCandidate + ")");
                    }
                    if (isRetriableDepositAddressError(ex)) {
                        continue;
                    }
                    throw ex;
                }
            }
        }
        if (last != null) {
            throw last;
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
            case "ETH", "ETHEREUM", "ERC20" -> "ERC20";
            case "TRX", "TRON", "TRC20" -> "TRC20";
            case "BSC", "BEP20", "BNBSMARTCHAIN" -> "BEP20";
            case "ARBITRUM", "ARBITRUMONE", "ARB" -> "ARBITRUM";
            case "POLYGON", "MATIC" -> "POLYGON";
            case "AVAXC", "AVALANCHECCHAIN" -> "AVAXC";
            case "SOL", "SOLANA" -> "SOL";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            default -> n;
        };
    }

    private SymbolMeta resolveSymbol(String base, String quote) {
        JsonNode data = requireOk(publicGet("/api/v2/symbols", Map.of()), "symbols").get("data");
        if (data == null || !data.isArray()) {
            throw new ExchangeException("Unexpected response from KuCoin symbols API");
        }
        String baseUpper = base.toUpperCase();
        String quoteUpper = quote.toUpperCase();
        for (JsonNode row : data) {
            String baseCcy = textOf(row, "baseCurrency");
            String quoteCcy = textOf(row, "quoteCurrency");
            if (!baseUpper.equalsIgnoreCase(baseCcy) || !quoteUpper.equalsIgnoreCase(quoteCcy)) {
                continue;
            }
            if (!row.path("enableTrading").asBoolean(true)) {
                continue;
            }
            String symbol = textOf(row, "symbol");
            BigDecimal baseIncrement = dec(row.get("baseIncrement"));
            BigDecimal baseMinSize = dec(row.get("baseMinSize"));
            BigDecimal quoteMinSize = dec(row.get("quoteMinSize"));
            return new SymbolMeta(symbol, baseIncrement, baseMinSize, quoteMinSize);
        }
        throw new ExchangeException("Invalid symbol: " + baseUpper + "/" + quoteUpper);
    }

    private CurrencyMeta resolveCurrency(String asset) {
        String currency = asset.toUpperCase();
        JsonNode root;
        try {
            root = requireOk(publicGet("/api/v3/currencies/" + currency, Map.of()), "currency");
        } catch (ExchangeException ex) {
            root = requireOk(publicGet("/api/v2/currencies/" + currency, Map.of()), "currency");
        }
        JsonNode data = root.get("data");
        if (data == null || data.isNull()) {
            throw new ExchangeException("KuCoin does not support asset: " + currency);
        }
        String ccy = StringUtils.upperCase(StringUtils.defaultIfBlank(textOf(data, "currency"), currency));
        List<ChainMeta> chains = new ArrayList<>();
        JsonNode chainList = data.get("chains");
        if (chainList != null && chainList.isArray()) {
            for (JsonNode chain : chainList) {
                String chainName = textOf(chain, "chainName", "chain");
                if (StringUtils.isBlank(chainName)) {
                    continue;
                }
                boolean dep = chain.path("isDepositEnabled").asBoolean(true);
                boolean wd = chain.path("isWithdrawEnabled").asBoolean(true);
                String chainId = textOf(chain, "chainId");
                chains.add(new ChainMeta(StringUtils.upperCase(chainName), StringUtils.upperCase(chainId), dep, wd));
            }
        }
        return new CurrencyMeta(ccy, chains);
    }

    private ChainMeta selectChain(CurrencyMeta currency, String requestedNetwork, boolean withdraw) {
        String req = normalizeDepositNetwork(requestedNetwork);
        List<ChainMeta> enabled = new ArrayList<>();
        for (ChainMeta chain : currency.chains) {
            if (withdraw ? chain.isWithdrawEnabled : chain.isDepositEnabled) {
                enabled.add(chain);
            }
        }

        if (StringUtils.isNotBlank(req)) {
            ChainMeta matched = null;
            for (ChainMeta c : currency.chains) {
                String n1 = normalizeDepositNetwork(c.chainName);
                String n2 = normalizeDepositNetwork(c.chainId);
                if (req.equals(n1) || req.equals(n2)
                        || (StringUtils.isNotBlank(n1) && (n1.contains(req) || req.contains(n1)))
                        || (StringUtils.isNotBlank(n2) && (n2.contains(req) || req.contains(n2)))) {
                    matched = c;
                    break;
                }
            }
            if (matched == null) {
                throw new ExchangeException("KuCoin does not support network " + requestedNetwork + " for asset " + currency.currency);
            }
            boolean isEnabled = withdraw ? matched.isWithdrawEnabled : matched.isDepositEnabled;
            if (!isEnabled) {
                throw new ExchangeException("KuCoin " + (withdraw ? "withdrawal" : "deposit") + " is disabled for network " + requestedNetwork + " (" + matched.chainName + ")");
            }
            return matched;
        }

        if (enabled.isEmpty()) {
            throw new ExchangeException("No " + (withdraw ? "withdraw" : "deposit") + " networks for asset " + currency.currency);
        }
        if (enabled.size() == 1) {
            return enabled.get(0);
        }
        StringBuilder sb = new StringBuilder();
        for (ChainMeta c : enabled) {
            String n1 = normalizeDepositNetwork(c.chainName);
            String n2 = normalizeDepositNetwork(c.chainId);
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append(c.chainName);
        }
        throw new ExchangeException("Network is required for " + currency.currency + " (available: " + sb + ")");
    }

    private JsonNode placeOrder(Map<String, Object> body) {
        JsonNode root = requireOk(signedPost("/api/v1/orders", body), "order");
        JsonNode data = root.get("data");
        if (data == null || data.isNull()) {
            throw new ExchangeException("Unexpected response from KuCoin order API");
        }
        return data;
    }

    private BigDecimal lastPrice(String symbol) {
        JsonNode root = requireOk(publicGet("/api/v1/market/orderbook/level1", Map.of("symbol", symbol)), "ticker");
        JsonNode data = root.get("data");
        return dec(data == null ? null : data.get("price"));
    }

    private BuyInfoResult impact(String symbol, BigDecimal quoteAmount, JsonNode levels, boolean buy) {
        if (levels == null || !levels.isArray()) {
            throw new ExchangeException("Unexpected response from KuCoin orderbook API");
        }
        BigDecimal rem = quoteAmount;
        BigDecimal quote = BigDecimal.ZERO;
        BigDecimal base = BigDecimal.ZERO;
        List<BuyInfoItem> items = new ArrayList<>();
        for (JsonNode level : levels) {
            if (!level.isArray() || level.size() < 2) {
                continue;
            }
            BigDecimal price = dec(level.get(0));
            BigDecimal qty = dec(level.get(1));
            if (price.signum() <= 0 || qty.signum() <= 0) {
                continue;
            }
            BigDecimal value = price.multiply(qty);
            if (rem.compareTo(value) >= 0) {
                base = base.add(qty);
                quote = quote.add(value);
                rem = rem.subtract(value);
                items.add(new BuyInfoItem(price, qty, value));
            } else {
                BigDecimal part = rem.divide(price, 18, RoundingMode.DOWN);
                if (part.signum() > 0) {
                    BigDecimal partial = part.multiply(price);
                    base = base.add(part);
                    quote = quote.add(partial);
                    items.add(new BuyInfoItem(price, part, partial));
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

    private List<OrderBookEntry> parseSide(JsonNode levels, int depth) {
        List<OrderBookEntry> out = new ArrayList<>();
        if (levels == null || !levels.isArray()) {
            return out;
        }
        int max = depth <= 0 ? Integer.MAX_VALUE : depth;
        for (int i = 0; i < levels.size() && i < max; i++) {
            JsonNode level = levels.get(i);
            if (!level.isArray() || level.size() < 2) {
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

    private String extractDepositAddress(JsonNode data, ChainMeta target) {
        if (data == null || data.isNull()) {
            return null;
        }
        if (data.isTextual()) {
            return StringUtils.trimToNull(data.asText());
        }
        if (data.isObject()) {
            String direct = textOf(data, "address");
            if (StringUtils.isNotBlank(direct)) {
                return direct;
            }
            JsonNode nested = data.get("data");
            if (nested != null) {
                String fromNested = extractDepositAddress(nested, target);
                if (StringUtils.isNotBlank(fromNested)) {
                    return fromNested;
                }
            }
        }
        if (data.isArray()) {
            String wantedName = normalizeDepositNetwork(target.chainName);
            String wantedId = normalizeDepositNetwork(target.chainId);
            for (JsonNode row : data) {
                String chain = normalizeDepositNetwork(textOf(row, "chain", "chainName"));
                if (StringUtils.isNotBlank(chain)
                        && StringUtils.isNotBlank(wantedName)
                        && !chain.equals(wantedName)
                        && !chain.equals(wantedId)
                        && !(chain.contains(wantedName) || wantedName.contains(chain))) {
                    continue;
                }
                String addr = textOf(row, "address");
                if (StringUtils.isNotBlank(addr)) {
                    return addr;
                }
            }
            for (JsonNode row : data) {
                String addr = textOf(row, "address");
                if (StringUtils.isNotBlank(addr)) {
                    return addr;
                }
            }
        }
        return null;
    }

    private boolean isRetriableDepositAddressError(ExchangeException ex) {
        String m = StringUtils.defaultString(ex.getUserMessage());
        return m.contains("code=900014")
                || m.contains("not exist")
                || m.contains("HTTP 404")
                || m.contains("HTTP 405")
                || m.contains("HTTP 400");
    }

    private boolean isDepositDisabledError(ExchangeException ex) {
        String m = StringUtils.defaultString(ex.getUserMessage()).toLowerCase();
        return m.contains("code=260200") || m.contains("deposit.disabled");
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String requestPath = params == null || params.isEmpty() ? path : path + "?" + qs(params);
        LOG.info("kucoin GET {}", LogSanitizer.sanitize(requestPath));
        String body = webClient.get()
                .uri(requestPath)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("KuCoin request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
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
        LOG.info("kucoin GET {}", LogSanitizer.sanitize(requestPath));
        String body = webClient.get()
                .uri(requestPath)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("KC-API-KEY", apiKey())
                .header("KC-API-SIGN", auth.sign)
                .header("KC-API-TIMESTAMP", auth.ts)
                .header("KC-API-PASSPHRASE", signedPassphrase())
                .header("KC-API-KEY-VERSION", "2")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("KuCoin request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "signed GET");
    }

    private JsonNode signedPost(String path, Map<String, Object> payload) {
        String json = toJson(payload);
        Auth auth = sign("POST", path, json);
        LOG.info("kucoin POST {}", LogSanitizer.sanitize(path + "?" + json));
        String body = webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header("KC-API-KEY", apiKey())
                .header("KC-API-SIGN", auth.sign)
                .header("KC-API-TIMESTAMP", auth.ts)
                .header("KC-API-PASSPHRASE", signedPassphrase())
                .header("KC-API-KEY-VERSION", "2")
                .bodyValue(json)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex ->
                        Mono.error(new ExchangeException("KuCoin request failed: HTTP " + ex.getStatusCode().value() + " body=" + ex.getResponseBodyAsString(), ex)))
                .block();
        return readJson(body, "signed POST");
    }

    private JsonNode requireOk(JsonNode root, String context) {
        if (root == null || root.isNull()) {
            throw new ExchangeException("Unexpected response from KuCoin " + context + " API");
        }
        String code = textOf(root, "code");
        if (StringUtils.isNotBlank(code) && !"200000".equals(code)) {
            String msg = textOf(root, "msg", "message");
            throw new ExchangeException("KuCoin " + context + " failed: code=" + code + " " + StringUtils.defaultString(msg));
        }
        return root;
    }

    private Auth sign(String method, String requestPath, String body) {
        String ts = String.valueOf(System.currentTimeMillis());
        String prehash = ts + method.toUpperCase() + requestPath + StringUtils.defaultString(body);
        String sign = base64HmacSha256(apiSecret(), prehash);
        return new Auth(ts, sign);
    }

    private String signedPassphrase() {
        return base64HmacSha256(apiSecret(), apiPassphrase());
    }

    private String base64HmacSha256(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return Base64.getEncoder().encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign KuCoin request", e);
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
            sb.append(enc(e.getKey())).append("=").append(enc(e.getValue()));
        }
        return sb.toString();
    }

    private String enc(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString()).replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode KuCoin query param", e);
        }
    }

    private JsonNode readJson(String body, String context) {
        try {
            return MAPPER.readTree(StringUtils.defaultIfBlank(body, "{}"));
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse KuCoin " + context + " response", e);
        }
    }

    private String toJson(Object payload) {
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize KuCoin payload", e);
        }
    }

    private String apiKey() {
        String value = secrets == null ? null : secrets.getApiKey();
        if (StringUtils.isBlank(value)) {
            throw new ExchangeException("Missing API credentials for kucoin");
        }
        return value;
    }

    private String apiSecret() {
        String value = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(value)) {
            throw new ExchangeException("Missing API credentials for kucoin");
        }
        return value;
    }

    private String apiPassphrase() {
        String value = secrets == null ? null : secrets.getApiPassphrase();
        if (StringUtils.isBlank(value)) {
            throw new ExchangeException("Missing API passphrase for kucoin");
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

    private record SymbolMeta(String symbol, BigDecimal baseIncrement, BigDecimal baseMinSize, BigDecimal minFunds) {
    }

    private record ChainMeta(String chainName, String chainId, boolean isDepositEnabled, boolean isWithdrawEnabled) {
    }

    private record CurrencyMeta(String currency, List<ChainMeta> chains) {
    }

    private enum EndpointAttempt {
        GET_V2,
        GET_V1,
        POST_V2,
        POST_V1
    }
}
