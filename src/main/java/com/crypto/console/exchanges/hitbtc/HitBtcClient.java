package com.crypto.console.exchanges.hitbtc;

import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.DepositNetworkNormalizer;
import com.crypto.console.common.exchange.DepositNetworkProvider;
import com.crypto.console.common.exchange.impl.BaseExchangeClient;
import com.crypto.console.common.model.*;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.util.LogSanitizer;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.util.UriComponentsBuilder;

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

@Slf4j
public class HitBtcClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {

    public HitBtcClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("hitbtc", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        JsonNode response = privateGet("/trading/balance", null);
        if (response == null || !response.isArray()) {
            throw new ExchangeException("Unexpected response from HitBTC trading balance API");
        }
        for (JsonNode item : response) {
            String currency = textOf(item, "currency");
            if (currency != null && currency.equalsIgnoreCase(asset)) {
                return new Balance(currency.toUpperCase(), toDecimal(item.get("available")), toDecimal(item.get("reserved")));
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /account/crypto/estimate-withdraw requires amount parameter");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        SymbolInfo symbol = resolveSymbol(base, quote);
        int limit = depth <= 0 ? 100 : Math.min(depth, 1000);
        JsonNode response = publicGet("/public/orderbook/" + symbol.symbol, Map.of("limit", String.valueOf(limit)));
        List<OrderBookEntry> bids = parseOrderBookSide(response == null ? null : response.get("bid"));
        List<OrderBookEntry> asks = parseOrderBookSide(response == null ? null : response.get("ask"));
        return new OrderBook(symbol.symbol, bids, asks);
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo symbol = resolveSymbol(base, quote);
        JsonNode response = publicGet("/public/orderbook/" + symbol.symbol, Map.of("limit", "1000"));
        JsonNode asks = response == null ? null : response.get("ask");
        if (asks == null || !asks.isArray()) {
            throw new ExchangeException("Unexpected response from HitBTC orderbook API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal spentQuote = BigDecimal.ZERO;
        BigDecimal boughtBase = BigDecimal.ZERO;
        List<BuyInfoItem> affected = new ArrayList<>();

        for (JsonNode level : asks) {
            BigDecimal price = toDecimal(level.get("price"));
            BigDecimal qty = toDecimal(level.get("size"));
            if (price.signum() <= 0 || qty.signum() <= 0) {
                continue;
            }
            BigDecimal levelCost = price.multiply(qty);
            if (remainingQuote.compareTo(levelCost) >= 0) {
                boughtBase = boughtBase.add(qty);
                spentQuote = spentQuote.add(levelCost);
                affected.add(new BuyInfoItem(price, qty, levelCost));
                remainingQuote = remainingQuote.subtract(levelCost);
            } else {
                BigDecimal partialQty = remainingQuote.divide(price, 18, RoundingMode.DOWN);
                if (partialQty.signum() > 0) {
                    BigDecimal partialCost = partialQty.multiply(price);
                    boughtBase = boughtBase.add(partialQty);
                    spentQuote = spentQuote.add(partialCost);
                    affected.add(new BuyInfoItem(price, partialQty, partialCost));
                }
                remainingQuote = BigDecimal.ZERO;
                break;
            }
        }

        if (boughtBase.signum() <= 0) {
            throw new ExchangeException("No ask liquidity available for " + symbol.symbol);
        }
        BigDecimal avg = spentQuote.divide(boughtBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol.symbol, quoteAmount, spentQuote, boughtBase, avg, List.copyOf(affected));
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo symbol = resolveSymbol(base, quote);
        JsonNode response = publicGet("/public/orderbook/" + symbol.symbol, Map.of("limit", "1000"));
        JsonNode bids = response == null ? null : response.get("bid");
        if (bids == null || !bids.isArray()) {
            throw new ExchangeException("Unexpected response from HitBTC orderbook API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal receivedQuote = BigDecimal.ZERO;
        BigDecimal soldBase = BigDecimal.ZERO;
        List<BuyInfoItem> affected = new ArrayList<>();

        for (JsonNode level : bids) {
            BigDecimal price = toDecimal(level.get("price"));
            BigDecimal qty = toDecimal(level.get("size"));
            if (price.signum() <= 0 || qty.signum() <= 0) {
                continue;
            }
            BigDecimal levelValue = price.multiply(qty);
            if (remainingQuote.compareTo(levelValue) >= 0) {
                soldBase = soldBase.add(qty);
                receivedQuote = receivedQuote.add(levelValue);
                affected.add(new BuyInfoItem(price, qty, levelValue));
                remainingQuote = remainingQuote.subtract(levelValue);
            } else {
                BigDecimal partialQty = remainingQuote.divide(price, 18, RoundingMode.DOWN);
                if (partialQty.signum() > 0) {
                    BigDecimal partialValue = partialQty.multiply(price);
                    soldBase = soldBase.add(partialQty);
                    receivedQuote = receivedQuote.add(partialValue);
                    affected.add(new BuyInfoItem(price, partialQty, partialValue));
                }
                remainingQuote = BigDecimal.ZERO;
                break;
            }
        }

        if (soldBase.signum() <= 0) {
            throw new ExchangeException("No bid liquidity available for " + symbol.symbol);
        }
        BigDecimal avg = receivedQuote.divide(soldBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol.symbol, quoteAmount, receivedQuote, soldBase, avg, List.copyOf(affected));
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        SymbolInfo symbol = resolveSymbol(base, quote);
        BigDecimal ask = getTickerPrice(symbol.symbol, true);
        if (ask == null || ask.signum() <= 0) {
            throw new ExchangeException("Unable to get ask price for " + symbol.symbol);
        }
        BigDecimal qty = quoteAmount.divide(ask, 18, RoundingMode.DOWN);
        qty = applyStep(qty, symbol.quantityIncrement);
        if (qty.signum() <= 0) {
            throw new ExchangeException("Buy quantity too low after applying lot size for " + symbol.symbol);
        }

        Map<String, String> form = new LinkedHashMap<>();
        form.put("symbol", symbol.symbol);
        form.put("side", "buy");
        form.put("type", "market");
        form.put("quantity", qty.toPlainString());
        JsonNode response = privatePostForm("/order", form);
        String orderId = textOf(response, "clientOrderId", "id");
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from HitBTC order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        SymbolInfo symbol = resolveSymbol(base, quote);
        BigDecimal qty = applyStep(baseAmount, symbol.quantityIncrement);
        if (qty.signum() <= 0) {
            throw new ExchangeException("Sell quantity too low after applying lot size for " + symbol.symbol);
        }

        BigDecimal bid = getTickerPrice(symbol.symbol, false);
        if (bid != null && bid.signum() > 0) {
            BigDecimal notional = qty.multiply(bid);
            if (notional.signum() <= 0) {
                throw new ExchangeException("Sell notional must be positive for " + symbol.symbol);
            }
        }

        Map<String, String> form = new LinkedHashMap<>();
        form.put("symbol", symbol.symbol);
        form.put("side", "sell");
        form.put("type", "market");
        form.put("quantity", qty.toPlainString());
        JsonNode response = privatePostForm("/order", form);
        String orderId = textOf(response, "clientOrderId", "id");
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from HitBTC order API");
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

        CurrencyTarget target = resolveCurrencyTarget(asset, network, true);
        Map<String, String> form = new LinkedHashMap<>();
        form.put("currency", target.currency);
        form.put("amount", amount.toPlainString());
        form.put("address", address);
        form.put("autoCommit", "true");
        if (StringUtils.isNotBlank(target.networkCode)) {
            form.put("networkCode", target.networkCode);
        }
        if (StringUtils.isNotBlank(memoOrNull)) {
            form.put("paymentId", memoOrNull);
        }

        JsonNode response = privatePostForm("/account/crypto/withdraw", form);
        String withdrawId = textOf(response, "id");
        if (StringUtils.isBlank(withdrawId)) {
            throw new ExchangeException("Missing withdrawal id from HitBTC withdraw API");
        }
        return new WithdrawResult(withdrawId, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public String getWithdrawStatus(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        List<CurrencyRecord> currencies = getCurrencies();
        String assetUpper = asset.toUpperCase();
        List<String> statuses = new ArrayList<>();
        for (CurrencyRecord c : currencies) {
            if (!isAssetFamilyMatch(assetUpper, c.currency)) {
                continue;
            }
            String name = StringUtils.defaultIfBlank(c.networkCode, c.currency);
            statuses.add(StringUtils.upperCase(name) + "=" + (c.payoutEnabled ? "enabled" : "disabled"));
        }
        if (statuses.isEmpty()) {
            return "withdraw status: unavailable";
        }
        return "withdraw status: " + String.join(", ", statuses);
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("No dedicated /time endpoint in HitBTC v2 docs");
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        List<CurrencyRecord> currencies = getCurrencies();
        Set<String> networks = new LinkedHashSet<>();
        String assetUpper = asset.toUpperCase();
        for (CurrencyRecord c : currencies) {
            if (!c.payinEnabled) {
                continue;
            }
            if (!isAssetFamilyMatch(assetUpper, c.currency)) {
                continue;
            }
            if (StringUtils.isNotBlank(c.networkCode)) {
                networks.add(c.networkCode.toUpperCase());
            }
            networks.add(c.currency.toUpperCase());
        }
        if (networks.isEmpty()) {
            throw new ExchangeException("HitBTC does not support deposits for asset: " + assetUpper);
        }
        return networks;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        CurrencyTarget target = resolveCurrencyTarget(asset, network, false);

        Map<String, String> params = new LinkedHashMap<>();
        if (StringUtils.isNotBlank(target.networkCode)) {
            params.put("networkCode", target.networkCode);
        }

        JsonNode response = privateGet("/account/crypto/address/" + target.currency, params);
        String address = textOf(response, "address");
        if (StringUtils.isBlank(address)) {
            throw new ExchangeException("No deposit address returned for " + asset.toUpperCase() + " " + network);
        }
        return address;
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, true, true, true, false, true);
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
            case "AVALANCHECCHAIN", "AVAXC", "AVAXCCHAIN" -> "AVAXC";
            case "BNBSMARTCHAIN", "BSC", "BEP20" -> "BSC";
            case "ETHEREUM", "ETH", "ERC20" -> "ERC20";
            case "TRON", "TRX", "TRC20" -> "TRC20";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL" -> "SOL";
            default -> cleaned;
        };
    }

    private SymbolInfo resolveSymbol(String base, String quote) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        String baseUpper = base.toUpperCase();
        String quoteUpper = quote.toUpperCase();
        JsonNode list = publicGet("/public/symbol", null);
        if (list == null || !list.isArray()) {
            throw new ExchangeException("Unexpected response from HitBTC symbols API");
        }
        List<String> similarBaseSymbols = new ArrayList<>();
        List<SymbolInfo> weakUsdCandidates = new ArrayList<>();
        List<SymbolInfo> prefixCandidates = new ArrayList<>();
        for (JsonNode item : list) {
            String b = textOf(item, "baseCurrency");
            String q = textOf(item, "quoteCurrency");
            String id = textOf(item, "id");
            if (StringUtils.isNotBlank(b) && StringUtils.isNotBlank(id)) {
                String bUpperLocal = b.toUpperCase();
                if (bUpperLocal.contains(baseUpper)) {
                    if (quoteMatches(quoteUpper, q, id) || isWeakUsdtToUsdMatch(quoteUpper, q, id)) {
                        similarBaseSymbols.add(id);
                    }
                }
            }
            if (baseUpper.equalsIgnoreCase(b) && quoteMatches(quoteUpper, q, id)) {
                BigDecimal step = toDecimal(item.get("quantityIncrement"));
                return new SymbolInfo(id, step.signum() > 0 ? step : null);
            }
            if (StringUtils.isBlank(b) || StringUtils.isBlank(q) || !quoteMatches(quoteUpper, q, id)) {
                if (baseUpper.equalsIgnoreCase(b) && isWeakUsdtToUsdMatch(quoteUpper, q, id)) {
                    BigDecimal step = toDecimal(item.get("quantityIncrement"));
                    weakUsdCandidates.add(new SymbolInfo(id, step.signum() > 0 ? step : null));
                }
                continue;
            }
            String bUpper = b.toUpperCase();
            if (bUpper.endsWith(baseUpper) && bUpper.length() > baseUpper.length()) {
                String prefix = bUpper.substring(0, bUpper.length() - baseUpper.length());
                    if (prefix.chars().allMatch(Character::isDigit)) {
                    BigDecimal step = toDecimal(item.get("quantityIncrement"));
                    prefixCandidates.add(new SymbolInfo(id, step.signum() > 0 ? step : null));
                }
            }
        }
        if (weakUsdCandidates.size() == 1) {
            return weakUsdCandidates.get(0);
        }
        if (!weakUsdCandidates.isEmpty()) {
            StringBuilder options = new StringBuilder();
            for (SymbolInfo info : weakUsdCandidates) {
                if (!options.isEmpty()) {
                    options.append(", ");
                }
                options.append(info.symbol);
            }
            throw new ExchangeException("Ambiguous USD fallback for " + baseUpper + "/" + quoteUpper + ". Candidates: " + options);
        }
        if (prefixCandidates.size() == 1) {
            return prefixCandidates.get(0);
        }
        if (!prefixCandidates.isEmpty()) {
            StringBuilder options = new StringBuilder();
            for (SymbolInfo info : prefixCandidates) {
                if (!options.isEmpty()) {
                    options.append(", ");
                }
                options.append(info.symbol);
            }
            throw new ExchangeException("Ambiguous symbol for " + baseUpper + "/" + quoteUpper + ". Did you mean: " + options + "?");
        }
        if (!similarBaseSymbols.isEmpty()) {
            StringBuilder options = new StringBuilder();
            int max = Math.min(similarBaseSymbols.size(), 8);
            for (int i = 0; i < max; i++) {
                if (i > 0) {
                    options.append(", ");
                }
                options.append(similarBaseSymbols.get(i));
            }
            throw new ExchangeException("Invalid symbol: " + baseUpper + "/" + quoteUpper + ". Similar markets: " + options);
        }
        throw new ExchangeException("Invalid symbol: " + baseUpper + "/" + quoteUpper);
    }

    private boolean quoteMatches(String requestedQuote, String symbolQuoteCurrency, String symbolId) {
        if (StringUtils.isBlank(requestedQuote) || StringUtils.isBlank(symbolQuoteCurrency)) {
            return false;
        }
        if (requestedQuote.equalsIgnoreCase(symbolQuoteCurrency)) {
            return true;
        }
        if ("USDT".equalsIgnoreCase(requestedQuote)
                && "USD".equalsIgnoreCase(symbolQuoteCurrency)
                && StringUtils.endsWithIgnoreCase(symbolId, "USDT")) {
            return true;
        }
        if ("USD".equalsIgnoreCase(requestedQuote)
                && "USD".equalsIgnoreCase(symbolQuoteCurrency)) {
            return true;
        }
        return false;
    }

    private boolean isWeakUsdtToUsdMatch(String requestedQuote, String symbolQuoteCurrency, String symbolId) {
        if (!"USDT".equalsIgnoreCase(requestedQuote)) {
            return false;
        }
        if (!"USD".equalsIgnoreCase(symbolQuoteCurrency)) {
            return false;
        }
        if (StringUtils.isBlank(symbolId)) {
            return false;
        }
        return StringUtils.endsWithIgnoreCase(symbolId, "USD")
                && !StringUtils.endsWithIgnoreCase(symbolId, "USDT");
    }

    private BigDecimal getTickerPrice(String symbol, boolean ask) {
        JsonNode ticker = publicGet("/public/ticker/" + symbol, null);
        if (ticker == null || !ticker.isObject()) {
            return null;
        }
        BigDecimal p = ask ? toDecimal(ticker.get("ask")) : toDecimal(ticker.get("bid"));
        if (p.signum() > 0) {
            return p;
        }
        return toDecimal(ticker.get("last"));
    }

    private List<OrderBookEntry> parseOrderBookSide(JsonNode side) {
        List<OrderBookEntry> result = new ArrayList<>();
        if (side == null || !side.isArray()) {
            return result;
        }
        for (JsonNode level : side) {
            BigDecimal price = toDecimal(level.get("price"));
            BigDecimal size = toDecimal(level.get("size"));
            if (price.signum() > 0 && size.signum() > 0) {
                result.add(new OrderBookEntry(price, size));
            }
        }
        return result;
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

    private CurrencyTarget resolveCurrencyTarget(String asset, String network, boolean forWithdraw) {
        String assetUpper = asset.toUpperCase();
        String normalizedNetwork = normalizeDepositNetwork(network);
        List<CurrencyRecord> candidates = new ArrayList<>();
        for (CurrencyRecord c : getCurrencies()) {
            if (!isAssetFamilyMatch(assetUpper, c.currency)) {
                continue;
            }
            if (forWithdraw && !c.payoutEnabled) {
                continue;
            }
            if (!forWithdraw && !c.payinEnabled) {
                continue;
            }
            candidates.add(c);
        }
        if (candidates.isEmpty()) {
            throw new ExchangeException("HitBTC does not support " + (forWithdraw ? "withdrawals" : "deposits") + " for asset: " + assetUpper);
        }

        if (StringUtils.isBlank(normalizedNetwork)) {
            CurrencyRecord exact = findExactCurrency(candidates, assetUpper);
            if (exact != null) {
                return new CurrencyTarget(exact.currency, exact.networkCode);
            }
            if (candidates.size() == 1) {
                CurrencyRecord only = candidates.get(0);
                return new CurrencyTarget(only.currency, only.networkCode);
            }
            StringBuilder available = new StringBuilder();
            for (CurrencyRecord c : candidates) {
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(StringUtils.defaultIfBlank(c.networkCode, c.currency));
            }
            throw new ExchangeException("Network is required for " + assetUpper + " (available: " + available + ")");
        }

        for (CurrencyRecord c : candidates) {
            String normalizedCode = normalizeDepositNetwork(c.networkCode);
            String normalizedCurrency = normalizeDepositNetwork(c.currency);
            if (normalizedNetwork.equalsIgnoreCase(normalizedCode) || normalizedNetwork.equalsIgnoreCase(normalizedCurrency)) {
                return new CurrencyTarget(c.currency, c.networkCode);
            }
            if (normalizedCurrency != null && normalizedCurrency.endsWith(normalizedNetwork)) {
                return new CurrencyTarget(c.currency, c.networkCode);
            }
        }

        throw new ExchangeException("HitBTC does not support network " + network + " for asset " + assetUpper);
    }

    private CurrencyRecord findExactCurrency(List<CurrencyRecord> candidates, String assetUpper) {
        for (CurrencyRecord c : candidates) {
            if (assetUpper.equalsIgnoreCase(c.currency)) {
                return c;
            }
        }
        return null;
    }

    private boolean isAssetFamilyMatch(String assetUpper, String currency) {
        if (StringUtils.isBlank(currency)) {
            return false;
        }
        String c = currency.toUpperCase();
        if (c.equals(assetUpper)) {
            return true;
        }
        return c.startsWith(assetUpper);
    }

    private List<CurrencyRecord> getCurrencies() {
        JsonNode response = publicGet("/public/currency", null);
        if (response == null || !response.isArray()) {
            throw new ExchangeException("Unexpected response from HitBTC currency API");
        }
        List<CurrencyRecord> result = new ArrayList<>();
        for (JsonNode node : response) {
            String id = textOf(node, "id");
            if (StringUtils.isBlank(id)) {
                continue;
            }
            String networkCode = textOf(node, "networkCode");
            boolean payinEnabled = boolOf(node, "payinEnabled");
            boolean payoutEnabled = boolOf(node, "payoutEnabled");
            result.add(new CurrencyRecord(id.toUpperCase(), StringUtils.upperCase(networkCode), payinEnabled, payoutEnabled));
        }
        return result;
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String uri = buildUri(path, params);
        LOG.info("hitbtc GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "HitBTC request failed: HTTP " + ex.getStatusCode().value();
                    String body = ex.getResponseBodyAsString();
                    if (StringUtils.isNotBlank(body)) {
                        msg += " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        assertNoApiError(response, "public request");
        return response;
    }

    private JsonNode privateGet(String path, Map<String, String> params) {
        String uri = buildUri(path, params);
        LOG.info("hitbtc GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.AUTHORIZATION, basicAuthHeader())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "HitBTC request failed: HTTP " + ex.getStatusCode().value();
                    String body = ex.getResponseBodyAsString();
                    if (StringUtils.isNotBlank(body)) {
                        msg += " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        assertNoApiError(response, "private request");
        return response;
    }

    private JsonNode privatePostForm(String path, Map<String, String> form) {
        String uri = buildUri(path, null);
        LOG.info("hitbtc POST {}", LogSanitizer.sanitize(uri + "?" + buildFormBody(form)));
        JsonNode response = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.AUTHORIZATION, basicAuthHeader())
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(buildFormBody(form))
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "HitBTC request failed: HTTP " + ex.getStatusCode().value();
                    String body = ex.getResponseBodyAsString();
                    if (StringUtils.isNotBlank(body)) {
                        msg += " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        assertNoApiError(response, "private request");
        return response;
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

    private String buildFormBody(Map<String, String> form) {
        UriComponentsBuilder b = UriComponentsBuilder.newInstance();
        if (form != null) {
            for (Map.Entry<String, String> e : form.entrySet()) {
                if (e.getValue() != null) {
                    b.queryParam(e.getKey(), e.getValue());
                }
            }
        }
        String encoded = b.build(true).getQuery();
        return encoded == null ? "" : encoded;
    }

    private void assertNoApiError(JsonNode response, String context) {
        if (response != null && response.has("error") && response.get("error").isObject()) {
            JsonNode err = response.get("error");
            String code = textOf(err, "code");
            String message = textOf(err, "message");
            String description = textOf(err, "description");
            throw new ExchangeException("HitBTC " + context + " failed: code=" + code + " message=" + message +
                    (StringUtils.isBlank(description) ? "" : " description=" + description));
        }
    }

    private String basicAuthHeader() {
        String apiKey = secrets == null ? null : secrets.getApiKey();
        String apiSecret = secrets == null ? null : secrets.getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for hitbtc");
        }
        String raw = apiKey + ":" + apiSecret;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
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

    private boolean boolOf(JsonNode node, String key) {
        return node != null && key != null && node.has(key) && node.get(key).asBoolean(false);
    }

    private BigDecimal toDecimal(JsonNode node) {
        if (node == null || node.isNull()) {
            return BigDecimal.ZERO;
        }
        String text = node.asText();
        if (StringUtils.isBlank(text)) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(text);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    private record SymbolInfo(String symbol, BigDecimal quantityIncrement) {
    }

    private record CurrencyRecord(String currency, String networkCode, boolean payinEnabled, boolean payoutEnabled) {
    }

    private record CurrencyTarget(String currency, String networkCode) {
    }
}
