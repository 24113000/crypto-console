package com.crypto.console.exchanges.bitmart;

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
public class BitMartClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String HMAC_ALGO = "HmacSHA256";

    public BitMartClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("bitmart", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        if (StringUtils.isBlank(apiKey)) {
            throw new ExchangeException("Missing API key for bitmart");
        }

        JsonNode response = getJson("/account/v1/wallet", apiKey);
        JsonNode wallet = requireOk(response, "wallet").path("data").path("wallet");
        if (wallet.isArray()) {
            for (JsonNode item : wallet) {
                String currency = item.hasNonNull("currency") ? item.get("currency").asText() : null;
                if (currency != null && currency.equalsIgnoreCase(asset)) {
                    BigDecimal free = toDecimal(item.get("available"));
                    BigDecimal locked = toDecimal(item.get("frozen"));
                    return new Balance(currency.toUpperCase(), free, locked);
                }
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /account/v1/currencies (public)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /spot/quotation/v3/books (public)");
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String symbol = (base + "_" + quote).toUpperCase();
        // Validate symbol exists.
        getSymbolInfo(symbol);

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("limit", "200");
        String uri = "/spot/quotation/v3/books?" + buildQueryString(params);
        JsonNode response = getJson(uri, null);
        JsonNode ok = requireOk(response, "order book");
        JsonNode data = ok.get("data");
        JsonNode asks = resolveAsksNode(data);
        if (asks == null || !asks.isArray()) {
            throw new ExchangeException("Unexpected response from BitMart order book API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal spentQuote = BigDecimal.ZERO;
        BigDecimal boughtBase = BigDecimal.ZERO;
        List<BuyInfoItem> affectedItems = new ArrayList<>();

        for (JsonNode ask : asks) {
            BigDecimal price;
            BigDecimal quantity;
            if (ask == null) {
                continue;
            }
            if (ask.isArray() && ask.size() >= 2) {
                price = toDecimal(ask.get(0));
                quantity = toDecimal(ask.get(1));
            } else if (ask.isObject()) {
                price = firstDecimal(ask, "price", "p");
                quantity = firstDecimal(ask, "size", "quantity", "qty", "amount", "q");
            } else {
                continue;
            }
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
            throw new ExchangeException("No ask liquidity available for " + symbol);
        }

        BigDecimal averagePrice = spentQuote.divide(boughtBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol, quoteAmount, spentQuote, boughtBase, averagePrice, List.copyOf(affectedItems));
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
        String apiMemo = getApiMemo();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret) || StringUtils.isBlank(apiMemo)) {
            throw new ExchangeException("Missing API credentials for bitmart");
        }

        CurrencyEntry entry = resolveCurrencyForWithdraw(asset, network);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("currency", entry.currency);
        body.put("amount", amount.toPlainString());
        body.put("destination", "To Digital Address");
        body.put("address", address);
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("address_memo", memoOrNull);
        }
        String bodyJson = toJson(body);
        JsonNode response = postSignedJson("/account/v1/withdraw/apply", bodyJson, apiKey, apiSecret, apiMemo);
        JsonNode ok = requireOk(response, "withdraw");
        JsonNode data = ok.get("data");
        String id = data != null && data.hasNonNull("withdraw_id") ? data.get("withdraw_id").asText() : null;
        if (StringUtils.isBlank(id) && data != null && data.hasNonNull("id")) {
            id = data.get("id").asText();
        }
        if (StringUtils.isBlank(id) && data != null && data.hasNonNull("withdrawId")) {
            id = data.get("withdrawId").asText();
        }
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from BitMart withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("TODO: verify BitMart server time endpoint");
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        Set<String> networks = new HashSet<>();
        String assetUpper = asset.toUpperCase();
        List<CurrencyEntry> entries = fetchCurrencies(assetUpper, getApiKey());
        boolean foundEnabled = false;
        for (CurrencyEntry entry : entries) {
            if (!entry.matchesAsset(assetUpper) || !entry.depositEnabled) {
                continue;
            }
            String net = entry.network;
            if (StringUtils.isBlank(net)) {
                net = entry.networkFromCurrency();
            }
            if (StringUtils.isNotBlank(net)) {
                networks.add(net.trim().toUpperCase());
                foundEnabled = true;
            }
        }
        if (!foundEnabled) {
            for (CurrencyEntry entry : entries) {
                if (!entry.matchesAsset(assetUpper)) {
                    continue;
                }
                String net = entry.network;
                if (StringUtils.isBlank(net)) {
                    net = entry.networkFromCurrency();
                }
                if (StringUtils.isNotBlank(net)) {
                    networks.add(net.trim().toUpperCase());
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
        String apiKey = getApiKey();
        if (StringUtils.isBlank(apiKey)) {
            throw new ExchangeException("Missing API key for bitmart");
        }

        CurrencyEntry entry = resolveCurrencyForDeposit(asset, network);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("currency", entry.currency);
        String uri = "/account/v1/deposit/address?" + buildQueryString(params);
        JsonNode response = getJson(uri, apiKey);
        JsonNode ok = requireOk(response, "deposit address");
        JsonNode data = ok.get("data");
        if (data != null && data.hasNonNull("address")) {
            return data.get("address").asText();
        }
        throw new ExchangeException("No deposit address returned for bitmart " + asset.toUpperCase());
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, false, true, true, false, true);
    }

    @Override
    public String normalizeDepositNetwork(String network) {
        if (StringUtils.isBlank(network)) {
            return null;
        }
        String candidate = network.trim();
        int openIdx = candidate.indexOf('(');
        int closeIdx = candidate.indexOf(')');
        if (openIdx >= 0 && closeIdx > openIdx) {
            candidate = candidate.substring(openIdx + 1, closeIdx);
        }
        String cleaned = candidate.replaceAll("[^A-Za-z0-9]", "").toUpperCase();
        return switch (cleaned) {
            case "ARBITRUMONE", "ARB" -> "ARBITRUM";
            case "AVALANCHECCHAIN", "AVAXCCHAIN", "AVAXC" -> "AVAXC";
            case "BNBSMARTCHAIN", "BSC", "BEP20", "BSCBNB" -> "BEP20";
            case "ETHEREUM", "ERC20", "ETH" -> "ERC20";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL", "SOLANASOL" -> "SOL";
            case "TRON", "TRC20", "TRX", "TRONTRC20" -> "TRC20";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "KAIA" -> "KAIA";
            case "CELO" -> "CELO";
            case "HECO" -> "HECO";
            default -> cleaned;
        };
    }

    private OrderResult submitMarketOrder(OrderSide side, String base, String quote, BigDecimal amount, boolean isQuoteAmount) {
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        String apiMemo = getApiMemo();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret) || StringUtils.isBlank(apiMemo)) {
            throw new ExchangeException("Missing API credentials for bitmart");
        }
        String symbol = (base + "_" + quote).toUpperCase();
        SymbolInfo info = getSymbolInfo(symbol);

        if (isQuoteAmount) {
            BigDecimal minQuote = info.minQuoteAmount;
            if (minQuote != null && minQuote.signum() > 0 && amount.compareTo(minQuote) < 0) {
                throw new ExchangeException("Order value " + amount + " below min notional " + minQuote + " for " + symbol);
            }
        } else {
            BigDecimal qty = applyLotSize(info, amount);
            if (qty.signum() <= 0) {
                throw new ExchangeException("Sell quantity below minimum lot size for " + symbol);
            }
            BigDecimal minQuote = info.minSellAmount;
            if (minQuote != null && minQuote.signum() > 0) {
                BigDecimal price = getLatestPrice(symbol);
                if (price != null && price.signum() > 0) {
                    BigDecimal notional = qty.multiply(price);
                    if (notional.compareTo(minQuote) < 0) {
                        throw new ExchangeException("Order value " + notional + " below min notional " + minQuote + " for " + symbol);
                    }
                }
            }
            amount = qty;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("symbol", symbol);
        body.put("side", side == OrderSide.BUY ? "buy" : "sell");
        body.put("type", "market");
        if (isQuoteAmount) {
            body.put("notional", amount.toPlainString());
        } else {
            body.put("size", amount.toPlainString());
        }

        String bodyJson = toJson(body);
        JsonNode response = postSignedJson("/spot/v2/submit_order", bodyJson, apiKey, apiSecret, apiMemo);
        JsonNode ok = requireOk(response, "order");
        JsonNode data = ok.get("data");
        String orderId = data != null && data.hasNonNull("orderId") ? data.get("orderId").asText() : null;
        if (StringUtils.isBlank(orderId) && data != null && data.hasNonNull("order_id")) {
            orderId = data.get("order_id").asText();
        }
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from BitMart order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market order submitted");
    }

    private SymbolInfo getSymbolInfo(String symbol) {
        JsonNode response = getJson("/spot/v1/symbols/details", null);
        JsonNode ok = requireOk(response, "symbols");
        JsonNode symbols = ok.path("data").path("symbols");
        if (symbols.isArray()) {
            for (JsonNode node : symbols) {
                String sym = node.hasNonNull("symbol") ? node.get("symbol").asText() : null;
                if (sym != null && sym.equalsIgnoreCase(symbol)) {
                    BigDecimal minQty = toDecimal(node.get("base_min_size"));
                    BigDecimal step = toDecimal(node.get("quote_increment"));
                    if (step.signum() <= 0) {
                        step = minQty;
                    }
                    BigDecimal minBuy = toDecimal(node.get("min_buy_amount"));
                    BigDecimal minSell = toDecimal(node.get("min_sell_amount"));
                    return new SymbolInfo(minQty, step, minBuy, minSell);
                }
            }
        }
        throw new ExchangeException("BitMart symbol not found: " + symbol);
    }

    private BigDecimal getLatestPrice(String symbol) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        String uri = "/spot/quotation/v3/ticker?" + buildQueryString(params);
        JsonNode response = getJson(uri, null);
        JsonNode ok = requireOk(response, "ticker");
        JsonNode data = ok.get("data");
        if (data != null && data.hasNonNull("last")) {
            return toDecimal(data.get("last"));
        }
        return null;
    }

    private BigDecimal applyLotSize(SymbolInfo info, BigDecimal quantity) {
        if (info == null || quantity == null) {
            return quantity == null ? BigDecimal.ZERO : quantity;
        }
        BigDecimal minQty = info.minQty;
        BigDecimal step = info.stepSize;
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
        return adjusted;
    }

    private CurrencyEntry resolveCurrencyForDeposit(String asset, String network) {
        return resolveCurrency(asset, network, true, true, false);
    }

    private CurrencyEntry resolveCurrencyForWithdraw(String asset, String network) {
        return resolveCurrency(asset, network, true, false, true);
    }

    private CurrencyEntry resolveCurrency(String asset, String network, boolean requireNetworkWhenMultiple, boolean forDeposit, boolean forWithdraw) {
        String assetUpper = asset.toUpperCase();
        List<CurrencyEntry> entries = fetchCurrencies(assetUpper, getApiKey());
        List<CurrencyEntry> matches = findMatchingEntries(entries, assetUpper, forDeposit, forWithdraw);
        if (matches.isEmpty()) {
            matches = findMatchingEntries(entries, assetUpper, false, false);
        }
        if (matches.isEmpty()) {
            throw new ExchangeException("BitMart does not support asset: " + assetUpper);
        }

        String normalized = normalizeDepositNetwork(network);
        if (StringUtils.isBlank(normalized)) {
            if (matches.size() == 1) {
                return matches.get(0);
            }
            if (requireNetworkWhenMultiple) {
                throw new ExchangeException("Multiple BitMart networks for " + assetUpper + "; specify network");
            }
            return matches.get(0);
        }

        for (CurrencyEntry entry : matches) {
            String entryNetwork = entry.network;
            if (StringUtils.isBlank(entryNetwork)) {
                entryNetwork = entry.networkFromCurrency();
            }
            if (StringUtils.isBlank(entryNetwork)) {
                continue;
            }
            if (normalizeDepositNetwork(entryNetwork).equalsIgnoreCase(normalized)) {
                return entry;
            }
        }

        throw new ExchangeException("BitMart does not support network " + network + " for asset " + assetUpper);
    }

    private boolean allowByStatus(CurrencyEntry entry, boolean forDeposit, boolean forWithdraw) {
        if (forDeposit && !entry.depositEnabled) {
            return false;
        }
        if (forWithdraw && !entry.withdrawEnabled) {
            return false;
        }
        return true;
    }

    private List<CurrencyEntry> findMatchingEntries(List<CurrencyEntry> entries, String assetUpper, boolean forDeposit, boolean forWithdraw) {
        List<CurrencyEntry> matches = new ArrayList<>();
        for (CurrencyEntry entry : entries) {
            if (entry.matchesAsset(assetUpper) && allowByStatus(entry, forDeposit, forWithdraw)) {
                matches.add(entry);
            }
        }
        return matches;
    }

    private List<CurrencyEntry> fetchCurrencies(String asset, String apiKey) {
        String assetUpper = StringUtils.isBlank(asset) ? null : asset.toUpperCase();
        List<CurrencyEntry> scoped = fetchCurrenciesInternal(assetUpper, apiKey);
        if (StringUtils.isBlank(assetUpper)) {
            return scoped;
        }
        for (CurrencyEntry entry : scoped) {
            if (entry.matchesAsset(assetUpper)) {
                return scoped;
            }
        }
        return fetchCurrenciesInternal(null, apiKey);
    }

    private List<CurrencyEntry> fetchCurrenciesInternal(String assetUpper, String apiKey) {
        Map<String, String> params = new LinkedHashMap<>();
        if (StringUtils.isNotBlank(assetUpper)) {
            params.put("currencies", assetUpper);
        }
        String uri = "/account/v1/currencies";
        if (!params.isEmpty()) {
            uri = uri + "?" + buildQueryString(params);
        }
        JsonNode response = getJson(uri, apiKey);
        JsonNode ok = requireOk(response, "currencies");
        JsonNode list = ok.path("data").path("currencies");
        List<CurrencyEntry> out = new ArrayList<>();
        if (list.isArray()) {
            for (JsonNode item : list) {
                String currency = item.hasNonNull("currency") ? item.get("currency").asText() : null;
                String name = item.hasNonNull("name") ? item.get("name").asText() : null;
                String network = item.hasNonNull("network") ? item.get("network").asText() : null;
                boolean depositEnabled = item.hasNonNull("deposit_enabled") && item.get("deposit_enabled").asBoolean();
                boolean withdrawEnabled = item.hasNonNull("withdraw_enabled") && item.get("withdraw_enabled").asBoolean();
                out.add(new CurrencyEntry(currency, name, network, depositEnabled, withdrawEnabled));
            }
        }
        return out;
    }

    private JsonNode getJson(String uri, String apiKey) {
        LOG.info("bitmart GET {}", LogSanitizer.sanitize(uri));
        org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec<?> req = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console");
        if (StringUtils.isNotBlank(apiKey)) {
            req = req.header("X-BM-KEY", apiKey);
        }
        String body = req.retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "BitMart request failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        if (StringUtils.isBlank(body)) {
            throw new ExchangeException("Empty response from BitMart");
        }
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse BitMart response", e);
        }
    }

    private JsonNode postSignedJson(String path, String bodyJson, String apiKey, String apiSecret, String apiMemo) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String signPayload = timestamp + "#" + apiMemo + "#" + bodyJson;
        String signature = sign(signPayload, apiSecret);

        LOG.info("bitmart POST {}", LogSanitizer.sanitize(path));
        String body = webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header("X-BM-KEY", apiKey)
                .header("X-BM-TIMESTAMP", timestamp)
                .header("X-BM-SIGN", signature)
                .bodyValue(bodyJson)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "BitMart request failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        if (StringUtils.isBlank(body)) {
            throw new ExchangeException("Empty response from BitMart");
        }
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse BitMart response", e);
        }
    }

    private String sign(String payload, String secret) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGO));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign BitMart request", e);
        }
    }

    private String buildQueryString(Map<String, String> params) {
        StringBuilder qs = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (!qs.isEmpty()) {
                qs.append("&");
            }
            qs.append(encodeQuery(entry.getKey()))
              .append("=")
              .append(encodeQuery(entry.getValue()));
        }
        return qs.toString();
    }

    private String encodeQuery(String value) {
        try {
            String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
            return encoded.replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode BitMart query parameter", e);
        }
    }

    private String toJson(Map<String, Object> body) {
        try {
            return MAPPER.writeValueAsString(body);
        } catch (Exception e) {
            throw new ExchangeException("Failed to build BitMart JSON body", e);
        }
    }

    private JsonNode requireOk(JsonNode node, String context) {
        if (node == null) {
            throw new ExchangeException("Empty response from BitMart " + context + " API");
        }
        int code = node.hasNonNull("code") ? node.get("code").asInt() : -1;
        if (code != 1000) {
            String msg = node.hasNonNull("message") ? node.get("message").asText() : "Unknown error";
            throw new ExchangeException("BitMart " + context + " API error: code=" + code + " msg=" + msg);
        }
        return node;
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

    private JsonNode resolveAsksNode(JsonNode data) {
        if (data == null) {
            return null;
        }
        if (data.has("asks")) {
            return data.get("asks");
        }
        if (data.has("sells")) {
            return data.get("sells");
        }
        if (data.has("ask")) {
            return data.get("ask");
        }
        return null;
    }

    private BigDecimal firstDecimal(JsonNode node, String... fields) {
        if (node == null || fields == null) {
            return BigDecimal.ZERO;
        }
        for (String field : fields) {
            if (node.has(field)) {
                BigDecimal value = toDecimal(node.get(field));
                return value;
            }
        }
        return BigDecimal.ZERO;
    }

    private String getApiKey() {
        return secrets == null ? null : secrets.getApiKey();
    }

    private String getApiSecret() {
        return secrets == null ? null : secrets.getApiSecret();
    }

    private String getApiMemo() {
        return secrets == null ? null : secrets.getApiMemo();
    }

    private record SymbolInfo(BigDecimal minQty, BigDecimal stepSize, BigDecimal minQuoteAmount, BigDecimal minSellAmount) {
    }

    private record CurrencyEntry(String currency, String name, String network, boolean depositEnabled, boolean withdrawEnabled) {
        boolean matchesAsset(String asset) {
            if (StringUtils.isBlank(currency)) {
                return false;
            }
            String base = currency;
            int dash = base.indexOf('-');
            if (dash > 0) {
                base = base.substring(0, dash);
            }
            return base.equalsIgnoreCase(asset) || (name != null && name.equalsIgnoreCase(asset));
        }

        String networkFromCurrency() {
            if (StringUtils.isBlank(currency)) {
                return null;
            }
            int dash = currency.indexOf('-');
            if (dash > 0 && dash + 1 < currency.length()) {
                return currency.substring(dash + 1);
            }
            return null;
        }
    }

    private enum OrderSide {
        BUY,
        SELL
    }
}






