package com.crypto.console.exchanges.ascendex;

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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Slf4j
public class AscendExClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private volatile String cachedAccountGroup;

    public AscendExClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("ascendex", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for ascendex");
        }

        String accountGroup = getAccountGroup(apiKey, apiSecret);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("asset", asset.toUpperCase());
        String query = buildQueryString(params);
        String path = "/" + accountGroup + "/api/pro/v1/cash/balance";
        String uri = query.isEmpty() ? path : path + "?" + query;

        JsonNode response = signedGet(uri, "balance", apiKey, apiSecret);
        JsonNode data = requireOk(response, "balance");
        if (data.isArray()) {
            for (JsonNode item : data) {
                String assetCode = textOf(item, "asset");
                if (assetCode != null && assetCode.equalsIgnoreCase(asset)) {
                    BigDecimal free = toDecimal(item.get("availableBalance"));
                    BigDecimal total = toDecimal(item.get("totalBalance"));
                    BigDecimal locked = total.subtract(free).max(BigDecimal.ZERO);
                    return new Balance(assetCode.toUpperCase(), free, locked);
                }
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("Use /api/pro/v2/assets (public) for withdrawal fee per chain");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /api/pro/v1/depth (public)");
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        ProductInfo product = resolveProduct(base, quote);
        String symbol = product.symbol;
        String uri = "/api/pro/v1/depth?symbol=" + symbol + "&n=200";
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "depth");
        JsonNode depth = data.has("data") ? data.get("data") : data;
        JsonNode asks = depth == null ? null : depth.get("asks");
        if (asks == null || !asks.isArray()) {
            throw new ExchangeException("Unexpected response from AscendEX depth API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal spentQuote = BigDecimal.ZERO;
        BigDecimal boughtBase = BigDecimal.ZERO;
        List<BuyInfoItem> affectedItems = new ArrayList<>();

        for (JsonNode ask : asks) {
            if (ask == null || !ask.isArray() || ask.size() < 2) {
                continue;
            }
            BigDecimal price = toDecimal(ask.get(0));
            BigDecimal quantity = toDecimal(ask.get(1));
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
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        ProductInfo product = resolveProduct(base, quote);
        String symbol = product.symbol;
        String uri = "/api/pro/v1/depth?symbol=" + symbol + "&n=200";
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "depth");
        JsonNode depth = data.has("data") ? data.get("data") : data;
        JsonNode bids = depth == null ? null : depth.get("bids");
        if (bids == null || !bids.isArray()) {
            throw new ExchangeException("Unexpected response from AscendEX depth API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal receivedQuote = BigDecimal.ZERO;
        BigDecimal soldBase = BigDecimal.ZERO;
        List<BuyInfoItem> affectedItems = new ArrayList<>();

        for (JsonNode bid : bids) {
            if (bid == null || !bid.isArray() || bid.size() < 2) {
                continue;
            }
            BigDecimal price = toDecimal(bid.get(0));
            BigDecimal quantity = toDecimal(bid.get(1));
            if (price.signum() <= 0 || quantity.signum() <= 0) {
                continue;
            }

            BigDecimal levelQuoteValue = price.multiply(quantity);
            if (remainingQuote.compareTo(levelQuoteValue) >= 0) {
                soldBase = soldBase.add(quantity);
                receivedQuote = receivedQuote.add(levelQuoteValue);
                affectedItems.add(new BuyInfoItem(price, quantity, levelQuoteValue));
                remainingQuote = remainingQuote.subtract(levelQuoteValue);
            } else {
                BigDecimal partialQty = remainingQuote.divide(price, 18, RoundingMode.DOWN);
                if (partialQty.signum() > 0) {
                    BigDecimal partialValue = partialQty.multiply(price);
                    soldBase = soldBase.add(partialQty);
                    receivedQuote = receivedQuote.add(partialValue);
                    affectedItems.add(new BuyInfoItem(price, partialQty, partialValue));
                }
                remainingQuote = BigDecimal.ZERO;
                break;
            }
            if (remainingQuote.signum() == 0) {
                break;
            }
        }

        if (soldBase.signum() <= 0) {
            throw new ExchangeException("No bid liquidity available for " + symbol);
        }

        BigDecimal averagePrice = receivedQuote.divide(soldBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol, quoteAmount, receivedQuote, soldBase, averagePrice, List.copyOf(affectedItems));
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        ProductInfo info = resolveProduct(base, quote);
        String symbol = info.symbol;

        BigDecimal price = getBestAskPrice(symbol);
        if (price == null || price.signum() <= 0) {
            throw new ExchangeException("Unable to fetch price for " + symbol);
        }

        BigDecimal qty = quoteAmount.divide(price, 18, RoundingMode.DOWN);
        qty = applyLotSize(info, qty);
        validateOrderQty(info, qty, "buy", symbol);
        if (info.minNotional != null && quoteAmount.compareTo(info.minNotional) < 0) {
            throw new ExchangeException("Order value " + quoteAmount + " below min notional " + info.minNotional + " for " + symbol);
        }

        return submitMarketOrder(symbol, "buy", qty);
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }

        ProductInfo info = resolveProduct(base, quote);
        String symbol = info.symbol;

        BigDecimal qty = applyLotSize(info, baseAmount);
        validateOrderQty(info, qty, "sell", symbol);

        BigDecimal price = getBestBidPrice(symbol);
        if (price != null && price.signum() > 0 && info.minNotional != null) {
            BigDecimal notional = qty.multiply(price);
            if (notional.compareTo(info.minNotional) < 0) {
                throw new ExchangeException("Order value " + notional + " below min notional " + info.minNotional + " for " + symbol);
            }
        }

        return submitMarketOrder(symbol, "sell", qty);
    }

    @Override
    public WithdrawResult withdraw(String asset, BigDecimal amount, String network, String address, String memoOrNull) {
        throw new ExchangeException("AscendEX withdrawal API requires enablement by support and is not publicly documented.");
    }

    @Override
    public ExchangeTime syncTime() {
        throw notImplemented("No dedicated server time endpoint documented for AscendEX");
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        List<AssetNetwork> networks = getAssetNetworks(asset);
        Set<String> result = new HashSet<>();
        for (AssetNetwork network : networks) {
            if (network.allowDeposit && StringUtils.isNotBlank(network.chainName)) {
                result.add(network.chainName.trim().toUpperCase());
            }
        }
        if (result.isEmpty()) {
            throw new ExchangeException("AscendEX does not support deposits for asset: " + asset.toUpperCase());
        }
        return result;
    }

    @Override
    public String getDepositAddress(String asset, String network) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for ascendex");
        }

        String normalized = normalizeDepositNetwork(network);
        List<String> candidates = networkCandidates(network, normalized);
        for (String candidate : candidates) {
            String address = fetchDepositAddress(asset, candidate, apiKey, apiSecret, true);
            if (StringUtils.isNotBlank(address)) {
                return address;
            }
        }
        return fetchDepositAddress(asset, normalized, apiKey, apiSecret, false);
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, false, true, false, false, true);
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
            case "AVALANCHECCHAIN", "AVAXCCHAIN", "AVAXC" -> "AVAXC";
            case "BNBSMARTCHAIN", "BSC", "BEP20" -> "BSC";
            case "ETHEREUM", "ERC20", "ETH" -> "ETH";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL" -> "SOL";
            case "TRON", "TRC20", "TRX" -> "TRON";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "ARBITRUM" -> "ARBITRUM";
            default -> cleaned;
        };
    }

    private OrderResult submitMarketOrder(String symbol, String side, BigDecimal orderQty) {
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for ascendex");
        }

        String accountGroup = getAccountGroup(apiKey, apiSecret);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("time", String.valueOf(System.currentTimeMillis()));
        payload.put("symbol", symbol);
        payload.put("orderQty", orderQty.stripTrailingZeros().toPlainString());
        payload.put("orderType", "market");
        payload.put("side", side);
        payload.put("respInst", "ACK");

        String body = toJson(payload);
        String path = "/" + accountGroup + "/api/pro/v1/cash/order";
        JsonNode response = signedPost(path, "order", body, apiKey, apiSecret);
        JsonNode data = requireOk(response, "order");
        String orderId = null;
        if (data.has("info")) {
            orderId = textOf(data.get("info"), "orderId", "id");
        }
        if (StringUtils.isBlank(orderId)) {
            orderId = textOf(data, "orderId", "id");
        }
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from AscendEX order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market order submitted");
    }

    private ProductInfo getProductInfo(String symbol) {
        List<ProductInfo> products = getProducts();
        for (ProductInfo info : products) {
            if (info.symbol != null && info.symbol.equalsIgnoreCase(symbol)) {
                return info;
            }
        }
        return null;
    }

    private ProductInfo resolveProduct(String base, String quote) {
        String baseUpper = base.toUpperCase();
        String quoteUpper = quote.toUpperCase();
        List<ProductInfo> products = getProducts();

        ProductInfo exact = null;
        for (ProductInfo info : products) {
            if (info.baseAsset != null && info.quoteAsset != null
                    && info.baseAsset.equalsIgnoreCase(baseUpper)
                    && info.quoteAsset.equalsIgnoreCase(quoteUpper)) {
                exact = info;
                break;
            }
        }
        if (exact != null) {
            return exact;
        }

        String directSymbol = (baseUpper + "/" + quoteUpper);
        for (ProductInfo info : products) {
            if (info.symbol != null && info.symbol.equalsIgnoreCase(directSymbol)) {
                return info;
            }
        }

        List<ProductInfo> suffixMatches = new ArrayList<>();
        for (ProductInfo info : products) {
            if (info.baseAsset == null || info.quoteAsset == null) {
                continue;
            }
            if (!info.quoteAsset.equalsIgnoreCase(quoteUpper)) {
                continue;
            }
            if (info.baseAsset.equalsIgnoreCase(baseUpper)) {
                continue;
            }
            if (info.baseAsset.toUpperCase().endsWith(baseUpper) && info.baseAsset.length() > baseUpper.length()) {
                String prefix = info.baseAsset.substring(0, info.baseAsset.length() - baseUpper.length());
                if (prefix.chars().allMatch(Character::isDigit)) {
                    suffixMatches.add(info);
                }
            }
        }
        if (suffixMatches.size() == 1) {
            return suffixMatches.get(0);
        }
        if (!suffixMatches.isEmpty()) {
            StringBuilder options = new StringBuilder();
            for (ProductInfo info : suffixMatches) {
                if (!options.isEmpty()) {
                    options.append(", ");
                }
                options.append(info.symbol);
            }
            throw new ExchangeException("Ambiguous symbol for " + baseUpper + "/" + quoteUpper +
                    ". Did you mean: " + options + "?");
        }

        throw new ExchangeException("Invalid symbol: " + baseUpper + "/" + quoteUpper);
    }

    private List<ProductInfo> getProducts() {
        String uri = "/api/pro/v1/cash/products";
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "products");
        if (!data.isArray()) {
            throw new ExchangeException("Unexpected response from AscendEX products API");
        }
        List<ProductInfo> products = new ArrayList<>();
        for (JsonNode item : data) {
            String symbol = textOf(item, "symbol");
            if (StringUtils.isBlank(symbol)) {
                continue;
            }
            String baseAsset = textOf(item, "baseAsset");
            String quoteAsset = textOf(item, "quoteAsset");
            if (StringUtils.isBlank(baseAsset) || StringUtils.isBlank(quoteAsset)) {
                String[] parts = symbol.split("/");
                if (parts.length == 2) {
                    baseAsset = StringUtils.defaultIfBlank(baseAsset, parts[0]);
                    quoteAsset = StringUtils.defaultIfBlank(quoteAsset, parts[1]);
                }
            }
            BigDecimal minQty = toDecimal(item.get("minQty"));
            BigDecimal maxQty = toDecimal(item.get("maxQty"));
            BigDecimal lotSize = toDecimal(item.get("lotSize"));
            BigDecimal minNotional = toDecimal(item.get("minNotional"));
            BigDecimal maxNotional = toDecimal(item.get("maxNotional"));
            products.add(new ProductInfo(symbol, baseAsset, quoteAsset, minQty, maxQty, lotSize, minNotional, maxNotional));
        }
        return products;
    }

    private BigDecimal getBestAskPrice(String symbol) {
        String uri = "/api/pro/v1/spot/ticker?symbol=" + symbol;
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "ticker");
        if (data.isArray() && data.size() > 0) {
            JsonNode item = data.get(0);
            BigDecimal ask = toDecimal(item.get("ask"));
            if (ask.signum() > 0) {
                return ask;
            }
            return toDecimal(item.get("p"));
        }
        return null;
    }

    private BigDecimal getBestBidPrice(String symbol) {
        String uri = "/api/pro/v1/spot/ticker?symbol=" + symbol;
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "ticker");
        if (data.isArray() && data.size() > 0) {
            JsonNode item = data.get(0);
            BigDecimal bid = toDecimal(item.get("bid"));
            if (bid.signum() > 0) {
                return bid;
            }
            return toDecimal(item.get("p"));
        }
        return null;
    }

    private BigDecimal applyLotSize(ProductInfo info, BigDecimal quantity) {
        if (info == null || quantity == null) {
            return quantity;
        }
        BigDecimal step = info.lotSize;
        if (step == null || step.signum() <= 0) {
            return quantity;
        }
        BigDecimal steps = quantity.divide(step, 0, RoundingMode.DOWN);
        BigDecimal adjusted = step.multiply(steps);
        return adjusted.stripTrailingZeros();
    }

    private void validateOrderQty(ProductInfo info, BigDecimal qty, String side, String symbol) {
        if (qty == null || qty.signum() <= 0) {
            throw new ExchangeException("" + side + " quantity below minimum lot size for " + symbol);
        }
        if (info == null) {
            return;
        }
        if (info.minQty != null && info.minQty.signum() > 0 && qty.compareTo(info.minQty) < 0) {
            throw new ExchangeException("" + side + " quantity " + qty + " below min quantity " + info.minQty + " for " + symbol);
        }
        if (info.maxQty != null && info.maxQty.signum() > 0 && qty.compareTo(info.maxQty) > 0) {
            throw new ExchangeException("" + side + " quantity " + qty + " above max quantity " + info.maxQty + " for " + symbol);
        }
    }

    private List<AssetNetwork> getAssetNetworks(String asset) {
        String uri = "/api/pro/v2/assets";
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "assets");
        if (!data.isArray()) {
            throw new ExchangeException("Unexpected response from AscendEX assets API");
        }
        for (JsonNode item : data) {
            String assetCode = textOf(item, "assetCode");
            if (assetCode != null && assetCode.equalsIgnoreCase(asset)) {
                List<AssetNetwork> networks = new ArrayList<>();
                JsonNode blockchains = item.get("blockChain");
                if (blockchains != null && blockchains.isArray()) {
                    for (JsonNode chain : blockchains) {
                        String chainName = textOf(chain, "chainName");
                        boolean allowDeposit = boolOf(chain, "allowDeposit", "allowDepoist");
                        boolean allowWithdraw = boolOf(chain, "allowWithdraw");
                        String withdrawFee = textOf(chain, "withdrawFee");
                        networks.add(new AssetNetwork(chainName, allowDeposit, allowWithdraw, withdrawFee));
                    }
                }
                return networks;
            }
        }
        return List.of();
    }

    private String fetchDepositAddress(String asset, String network, String apiKey, String apiSecret, boolean includeNetwork) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("asset", asset.toUpperCase());
        if (includeNetwork && StringUtils.isNotBlank(network)) {
            params.put("blockchain", network);
        }
        String query = buildQueryString(params);
        String uri = "/api/pro/v1/wallet/deposit/address" + (query.isEmpty() ? "" : "?" + query);
        JsonNode response = signedGet(uri, "wallet/deposit/address", apiKey, apiSecret);
        JsonNode data = requireOk(response, "deposit address");
        JsonNode addresses = data.has("address") ? data.get("address") : data;
        if (addresses == null || !addresses.isArray()) {
            return null;
        }
        if (addresses.size() == 1 && StringUtils.isBlank(network)) {
            JsonNode item = addresses.get(0);
            return textOf(item, "address");
        }
        for (JsonNode item : addresses) {
            String chain = textOf(item, "blockchain", "chain");
            if (StringUtils.isBlank(network) || networksMatch(chain, network)) {
                String addr = textOf(item, "address");
                if (StringUtils.isNotBlank(addr)) {
                    return addr;
                }
            }
        }
        return null;
    }

    private boolean networksMatch(String a, String b) {
        if (StringUtils.isBlank(a) || StringUtils.isBlank(b)) {
            return false;
        }
        String na = normalizeDepositNetwork(a);
        String nb = normalizeDepositNetwork(b);
        return na.equalsIgnoreCase(nb);
    }

    private List<String> networkCandidates(String raw, String normalized) {
        java.util.LinkedHashSet<String> candidates = new java.util.LinkedHashSet<>();
        if (StringUtils.isNotBlank(raw)) {
            candidates.add(raw.trim());
        }
        if (StringUtils.isNotBlank(normalized)) {
            candidates.add(normalized);
        }
        if ("BSC".equalsIgnoreCase(normalized)) {
            candidates.add("BEP20");
        }
        if ("TRON".equalsIgnoreCase(normalized)) {
            candidates.add("TRC20");
        }
        return new java.util.ArrayList<>(candidates);
    }

    private String getAccountGroup(String apiKey, String apiSecret) {
        if (cachedAccountGroup != null) {
            return cachedAccountGroup;
        }
        synchronized (this) {
            if (cachedAccountGroup != null) {
                return cachedAccountGroup;
            }
            JsonNode response = signedGet("/api/pro/v1/info", "info", apiKey, apiSecret);
            JsonNode data = requireOk(response, "account info");
            String group = textOf(data, "accountGroup", "account_group");
            if (StringUtils.isBlank(group)) {
                throw new ExchangeException("Missing accountGroup from AscendEX account info API");
            }
            cachedAccountGroup = group;
            return cachedAccountGroup;
        }
    }

    private JsonNode signedGet(String uri, String apiPath, String apiKey, String apiSecret) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String signature = sign(apiSecret, timestamp + "+" + apiPath);
        LOG.info("ascendex GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("x-auth-key", apiKey)
                .header("x-auth-signature", signature)
                .header("x-auth-timestamp", timestamp)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "AscendEX request failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode signedPost(String uri, String apiPath, String body, String apiKey, String apiSecret) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String signature = sign(apiSecret, timestamp + "+" + apiPath);
        LOG.info("ascendex POST {}", LogSanitizer.sanitize(uri));
        return webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header("x-auth-key", apiKey)
                .header("x-auth-signature", signature)
                .header("x-auth-timestamp", timestamp)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String bodyText = ex.getResponseBodyAsString();
                    String msg = "AscendEX request failed: HTTP " + ex.getStatusCode().value();
                    if (bodyText != null && !bodyText.isBlank()) {
                        msg = msg + " body=" + bodyText;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode publicGet(String uri) {
        LOG.info("ascendex GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String bodyText = ex.getResponseBodyAsString();
                    String msg = "AscendEX request failed: HTTP " + ex.getStatusCode().value();
                    if (bodyText != null && !bodyText.isBlank()) {
                        msg = msg + " body=" + bodyText;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode requireOk(JsonNode response, String context) {
        if (response == null) {
            throw new ExchangeException("Unexpected response from AscendEX " + context + " API");
        }
        if (response.has("code")) {
            int code = response.get("code").asInt(-1);
            if (code != 0) {
                String msg = textOf(response, "msg", "message");
                throw new ExchangeException("AscendEX " + context + " failed: code=" + code + " " + msg);
            }
        }
        if (response.has("data")) {
            return response.get("data");
        }
        return response;
    }

    private String sign(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(raw);
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign AscendEX request", e);
        }
    }

    private String buildQueryString(Map<String, String> params) {
        Map<String, String> sorted = new TreeMap<>(params);
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (!sb.isEmpty()) {
                sb.append("&");
            }
            sb.append(encodeQuery(entry.getKey()))
                    .append("=")
                    .append(encodeQuery(entry.getValue()));
        }
        return sb.toString();
    }

    private String encodeQuery(String value) {
        try {
            return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8.toString()).replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode AscendEX query parameter", e);
        }
    }

    private String toJson(Object payload) {
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize AscendEX request body", e);
        }
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

    private boolean boolOf(JsonNode node, String... keys) {
        if (node == null || keys == null) {
            return false;
        }
        for (String key : keys) {
            if (node.has(key)) {
                return node.get(key).asBoolean(false);
            }
        }
        return false;
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

    private String getApiKey() {
        if (secrets == null) {
            return null;
        }
        return StringUtils.trimToNull(secrets.getApiKey());
    }

    private String getApiSecret() {
        if (secrets == null) {
            return null;
        }
        return StringUtils.trimToNull(secrets.getApiSecret());
    }

    private record ProductInfo(String symbol, String baseAsset, String quoteAsset,
                               BigDecimal minQty, BigDecimal maxQty, BigDecimal lotSize,
                               BigDecimal minNotional, BigDecimal maxNotional) {
    }

    private record AssetNetwork(String chainName, boolean allowDeposit, boolean allowWithdraw, String withdrawFee) {
    }
}
