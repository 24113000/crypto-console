package com.crypto.console.exchanges.htx;

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
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class HtxClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HUOBI_TS_FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss").withZone(ZoneId.of("Z"));
    private volatile String cachedSpotAccountId;
    private volatile long timeOffsetMillis;
    private volatile long lastTimeSyncMillis;
    private volatile WebClient altWebClient;
    private volatile String altBaseUrl;

    public HtxClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("htx", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for htx");
        }

        String accountId = getSpotAccountId(apiKey, apiSecret);
        JsonNode response = signedGet("/v1/account/accounts/" + accountId + "/balance", null, apiKey, apiSecret);
        JsonNode data = requireOk(response, "account balance").path("data");
        JsonNode list = data.path("list");
        BigDecimal free = BigDecimal.ZERO;
        BigDecimal locked = BigDecimal.ZERO;
        if (list.isArray()) {
            for (JsonNode entry : list) {
                String currency = entry.hasNonNull("currency") ? entry.get("currency").asText() : null;
                if (currency == null || !currency.equalsIgnoreCase(asset)) {
                    continue;
                }
                String type = entry.hasNonNull("type") ? entry.get("type").asText() : null;
                BigDecimal amount = toDecimal(entry.get("balance"));
                if ("trade".equalsIgnoreCase(type)) {
                    free = free.add(amount);
                } else if ("frozen".equalsIgnoreCase(type)) {
                    locked = locked.add(amount);
                }
            }
        }
        return new Balance(asset.toUpperCase(), free, locked);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /v2/reference/currencies (signed)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        String symbol = resolveSymbol(base, quote);
        JsonNode tick = fetchDepthTick(symbol);
        JsonNode bidsNode = tick.path("bids");
        JsonNode asksNode = tick.path("asks");
        if (!bidsNode.isArray() || !asksNode.isArray()) {
            throw new ExchangeException("Unexpected response from HTX depth API");
        }
        int maxDepth = depth > 0 ? depth : 10;
        List<OrderBookEntry> bids = toOrderBookEntries(bidsNode, maxDepth);
        List<OrderBookEntry> asks = toOrderBookEntries(asksNode, maxDepth);
        return new OrderBook(symbol.toUpperCase(), bids, asks);
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String symbol = resolveSymbol(base, quote);
        JsonNode asks = fetchDepthTick(symbol).path("asks");
        if (!asks.isArray()) {
            throw new ExchangeException("Unexpected response from HTX depth API");
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
            throw new ExchangeException("No ask liquidity available for " + symbol.toUpperCase());
        }

        BigDecimal averagePrice = spentQuote.divide(boughtBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol.toUpperCase(), quoteAmount, spentQuote, boughtBase, averagePrice, List.copyOf(affectedItems));
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String symbol = resolveSymbol(base, quote);
        JsonNode bids = fetchDepthTick(symbol).path("bids");
        if (!bids.isArray()) {
            throw new ExchangeException("Unexpected response from HTX depth API");
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
            throw new ExchangeException("No bid liquidity available for " + symbol.toUpperCase());
        }

        BigDecimal averagePrice = receivedQuote.divide(soldBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(symbol.toUpperCase(), quoteAmount, receivedQuote, soldBase, averagePrice, List.copyOf(affectedItems));
    }

    @Override
    public OrderResult marketBuy(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for htx");
        }

        String symbol = (base + quote).toLowerCase();
        String accountId = getSpotAccountId(apiKey, apiSecret);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("account-id", accountId);
        body.put("symbol", symbol);
        body.put("type", "buy-market");
        body.put("amount", quoteAmount.toPlainString());

        JsonNode response = signedPost("/v1/order/orders/place", null, body, apiKey, apiSecret);
        JsonNode data = requireOk(response, "market buy").path("data");
        String orderId = data.isTextual() ? data.asText() : data.asText(null);
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from HTX order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market buy submitted");
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for htx");
        }

        String symbol = (base + quote).toLowerCase();
        SymbolInfo symbolInfo = getSymbolInfo(symbol);
        BigDecimal quantity = applyAmountPrecision(baseAmount, symbolInfo == null ? null : symbolInfo.amountPrecision);
        if (quantity.signum() <= 0) {
            throw new ExchangeException("Sell quantity below minimum lot size for " + symbol);
        }
        BigDecimal minQty = symbolInfo == null ? null : symbolInfo.sellMarketMinOrderAmt;
        if (minQty == null || minQty.signum() <= 0) {
            minQty = symbolInfo == null ? null : symbolInfo.minOrderAmt;
        }
        if (minQty != null && minQty.signum() > 0 && quantity.compareTo(minQty) < 0) {
            throw new ExchangeException("Sell quantity " + quantity + " below min order amount " + minQty + " for " + symbol);
        }
        BigDecimal minOrderValue = symbolInfo == null ? null : symbolInfo.minOrderValue;
        if (minOrderValue != null && minOrderValue.signum() > 0) {
            BigDecimal price = getMergedPrice(symbol);
            if (price != null && price.signum() > 0) {
                BigDecimal notional = quantity.multiply(price);
                if (notional.compareTo(minOrderValue) < 0) {
                    throw new ExchangeException("Order value " + notional + " below min notional " + minOrderValue + " for " + symbol);
                }
            }
        }

        String accountId = getSpotAccountId(apiKey, apiSecret);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("account-id", accountId);
        body.put("symbol", symbol);
        body.put("type", "sell-market");
        body.put("amount", quantity.toPlainString());

        JsonNode response = signedPost("/v1/order/orders/place", null, body, apiKey, apiSecret);
        JsonNode data = requireOk(response, "market sell").path("data");
        String orderId = data.isTextual() ? data.asText() : data.asText(null);
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from HTX order API");
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
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for htx");
        }

        ChainInfo chainInfo = resolveWithdrawChain(asset, network, apiKey, apiSecret);
        if (chainInfo == null || StringUtils.isBlank(chainInfo.chain)) {
            throw new ExchangeException("HTX does not support withdrawals for asset: " + asset.toUpperCase());
        }
        if (StringUtils.isNotBlank(network) && StringUtils.isBlank(chainInfo.chain)) {
            throw new ExchangeException("HTX does not support network " + network + " for asset " + asset.toUpperCase());
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("address", address);
        body.put("amount", amount.toPlainString());
        body.put("currency", asset.toLowerCase());
        body.put("chain", chainInfo.chain);
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("addr-tag", memoOrNull);
        }

        JsonNode response = signedPost("/v1/dw/withdraw/api/create", null, body, apiKey, apiSecret);
        JsonNode data = requireOk(response, "withdraw").path("data");
        String id = data.isTextual() ? data.asText() : data.asText(null);
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from HTX withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        long serverTime = fetchServerTimestamp();
        long now = System.currentTimeMillis();
        long offset = serverTime - now;
        return new ExchangeTime(serverTime, offset);
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for htx");
        }

        List<ChainInfo> chains = getChainInfo(asset, apiKey, apiSecret);
        if (chains.isEmpty()) {
            throw new ExchangeException("HTX does not support deposits for asset: " + asset.toUpperCase());
        }
        Set<String> networks = new HashSet<>();
        for (ChainInfo chain : chains) {
            if (chain.depositEnabled) {
                if (StringUtils.isNotBlank(chain.chain)) {
                    networks.add(chain.chain.trim().toUpperCase());
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
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for htx");
        }

        Map<String, String> params = new TreeMap<>();
        params.put("currency", asset.toLowerCase());
        JsonNode response = signedGet("/v2/account/deposit/address", params, apiKey, apiSecret);
        JsonNode data = extractData(response, "deposit address");
        if (!data.isArray()) {
            return null;
        }
        String normalized = normalizeNetworkForMatch(network, asset);
        for (JsonNode item : data) {
            String chain = item.hasNonNull("chain") ? item.get("chain").asText() : null;
            String address = item.hasNonNull("address") ? item.get("address").asText() : null;
            if (StringUtils.isBlank(address)) {
                continue;
            }
            if (StringUtils.isBlank(normalized)) {
                return address;
            }
            if (networksMatch(chain, asset, normalized)) {
                return address;
            }
        }
        return null;
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, true, true, true, true, false, true);
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
            case "ETHEREUM", "ERC20", "ETH" -> "ERC20";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL", "SOLANASOL" -> "SOL";
            case "TRON", "TRC20", "TRX", "TRONTRC20" -> "TRC20";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "HECO", "HRC20" -> "HRC20";
            case "KAIA" -> "KAIA";
            case "CELO" -> "CELO";
            default -> cleaned;
        };
    }

    private boolean networksMatch(String chain, String asset, String normalizedNetwork) {
        if (StringUtils.isBlank(chain) || StringUtils.isBlank(normalizedNetwork)) {
            return false;
        }
        String normalizedChain = normalizeChainForMatch(chain, asset);
        if (normalizedNetwork.equalsIgnoreCase(normalizedChain)) {
            return true;
        }
        String cleanedChain = normalizeDepositNetwork(chain);
        return normalizedNetwork.equalsIgnoreCase(cleanedChain);
    }

    private String normalizeNetworkForMatch(String network, String asset) {
        String normalized = normalizeDepositNetwork(network);
        if (StringUtils.isBlank(normalized) || StringUtils.isBlank(asset)) {
            return normalized;
        }
        String assetUpper = asset.toUpperCase();
        if (normalized.endsWith(assetUpper) && normalized.length() > assetUpper.length()) {
            return normalized.substring(0, normalized.length() - assetUpper.length());
        }
        return normalized;
    }

    private String normalizeChainForMatch(String chain, String asset) {
        if (StringUtils.isBlank(chain)) {
            return null;
        }
        String upper = chain.toUpperCase();
        if (StringUtils.isNotBlank(asset)) {
            String assetUpper = asset.toUpperCase();
            if (upper.endsWith(assetUpper) && upper.length() > assetUpper.length()) {
                String trimmed = upper.substring(0, upper.length() - assetUpper.length());
                String normalized = normalizeDepositNetwork(trimmed);
                if (StringUtils.isNotBlank(normalized)) {
                    return normalized;
                }
            }
        }
        return normalizeDepositNetwork(upper);
    }

    private ChainInfo resolveWithdrawChain(String asset, String network, String apiKey, String apiSecret) {
        List<ChainInfo> chains = getChainInfo(asset, apiKey, apiSecret);
        if (chains.isEmpty()) {
            return null;
        }
        String normalizedTarget = normalizeNetworkForMatch(network, asset);
        ChainInfo fallback = null;
        for (ChainInfo chain : chains) {
            if (!chain.withdrawEnabled) {
                continue;
            }
            if (chain.isDefault) {
                fallback = chain;
            }
            if (StringUtils.isBlank(normalizedTarget)) {
                continue;
            }
            if (networksMatch(chain.chain, asset, normalizedTarget)
                    || (StringUtils.isNotBlank(chain.displayName) && normalizedTarget.equalsIgnoreCase(normalizeDepositNetwork(chain.displayName)))) {
                return chain;
            }
        }
        if (StringUtils.isBlank(normalizedTarget)) {
            return fallback != null ? fallback : chains.get(0);
        }
        return null;
    }

    private List<ChainInfo> getChainInfo(String asset, String apiKey, String apiSecret) {
        String uri = "/v1/settings/common/chains?currency=" + encodeQuery(asset.toLowerCase());
        LOG.info("htx GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "chains").path("data");
        if (!data.isArray()) {
            return java.util.Collections.emptyList();
        }
        List<ChainInfo> chains = new ArrayList<>();
        for (JsonNode item : data) {
            String chain = item.hasNonNull("chain") ? item.get("chain").asText() : null;
            String currency = item.hasNonNull("currency") ? item.get("currency").asText() : null;
            if (currency != null && !currency.equalsIgnoreCase(asset)) {
                continue;
            }
            String depositStatus = item.hasNonNull("depositStatus") ? item.get("depositStatus").asText()
                    : (item.hasNonNull("de") ? item.get("de").asText() : null);
            String withdrawStatus = item.hasNonNull("withdrawStatus") ? item.get("withdrawStatus").asText()
                    : (item.hasNonNull("we") ? item.get("we").asText() : null);
            boolean depositEnabled = isStatusAllowed(depositStatus);
            boolean withdrawEnabled = isStatusAllowed(withdrawStatus);
            boolean isDefault = item.hasNonNull("default") && item.get("default").asBoolean(false);
            String displayName = item.hasNonNull("displayName") ? item.get("displayName").asText()
                    : (item.hasNonNull("dn") ? item.get("dn").asText() : null);
            chains.add(new ChainInfo(chain, displayName, depositEnabled, withdrawEnabled, isDefault));
        }
        return chains;
    }

    private boolean isStatusAllowed(String status) {
        if (StringUtils.isBlank(status)) {
            return false;
        }
        return "allowed".equalsIgnoreCase(status) || "true".equalsIgnoreCase(status) || "1".equals(status);
    }

    private String getSpotAccountId(String apiKey, String apiSecret) {
        if (StringUtils.isNotBlank(cachedSpotAccountId)) {
            return cachedSpotAccountId;
        }
        Map<String, String> params = new LinkedHashMap<>();
        params.put("account-type", "spot");
        JsonNode response = signedGet("/v1/account/accounts", params, apiKey, apiSecret);
        JsonNode data = requireOk(response, "account list").path("data");
        if (!data.isArray()) {
            throw new ExchangeException("Unexpected response from HTX account list API");
        }
        for (JsonNode account : data) {
            String type = account.hasNonNull("type") ? account.get("type").asText() : null;
            String state = account.hasNonNull("state") ? account.get("state").asText() : null;
            if ("spot".equalsIgnoreCase(type) && ("working".equalsIgnoreCase(state) || StringUtils.isBlank(state))) {
                String id = account.hasNonNull("id") ? account.get("id").asText() : null;
                if (StringUtils.isNotBlank(id)) {
                    cachedSpotAccountId = id;
                    return id;
                }
            }
        }
        throw new ExchangeException("No spot account found for HTX");
    }

    private SymbolInfo getSymbolInfo(String symbol) {
        LOG.info("htx GET {}", LogSanitizer.sanitize("/v1/common/symbols"));
        JsonNode response = publicGet("/v1/common/symbols");
        JsonNode data = requireOk(response, "symbols").path("data");
        if (!data.isArray()) {
            return null;
        }
        for (JsonNode item : data) {
            String sym = item.hasNonNull("symbol") ? item.get("symbol").asText() : null;
            if (sym != null && sym.equalsIgnoreCase(symbol)) {
                Integer amountPrecision = item.hasNonNull("amount-precision") ? item.get("amount-precision").asInt() : null;
                BigDecimal minOrderValue = toDecimal(item.get("min-order-value"));
                BigDecimal minOrderAmt = toDecimal(item.get("min-order-amt"));
                BigDecimal sellMarketMinOrderAmt = toDecimal(item.get("sell-market-min-order-amt"));
                return new SymbolInfo(amountPrecision, minOrderValue, minOrderAmt, sellMarketMinOrderAmt);
            }
        }
        return null;
    }

    private String resolveSymbol(String base, String quote) {
        String symbol = (base + quote).toLowerCase();
        if (getSymbolInfo(symbol) == null) {
            throw new ExchangeException("Invalid symbol: " + (base + quote).toUpperCase() + ". Check base/quote assets.");
        }
        return symbol;
    }

    private JsonNode fetchDepthTick(String symbol) {
        String uri = "/market/depth?symbol=" + encodeQuery(symbol.toLowerCase()) + "&type=step0";
        LOG.info("htx GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = publicGet(uri);
        JsonNode tick = requireOk(response, "depth").path("tick");
        if (tick.isMissingNode() || tick.isNull()) {
            throw new ExchangeException("Unexpected response from HTX depth API");
        }
        return tick;
    }

    private List<OrderBookEntry> toOrderBookEntries(JsonNode levels, int depth) {
        List<OrderBookEntry> entries = new ArrayList<>();
        if (!levels.isArray() || depth <= 0) {
            return entries;
        }
        int count = 0;
        for (JsonNode level : levels) {
            if (level == null || !level.isArray() || level.size() < 2) {
                continue;
            }
            BigDecimal price = toDecimal(level.get(0));
            BigDecimal quantity = toDecimal(level.get(1));
            if (price.signum() <= 0 || quantity.signum() <= 0) {
                continue;
            }
            entries.add(new OrderBookEntry(price, quantity));
            count++;
            if (count >= depth) {
                break;
            }
        }
        return entries;
    }

    private BigDecimal applyAmountPrecision(BigDecimal qty, Integer precision) {
        if (qty == null) {
            return BigDecimal.ZERO;
        }
        if (precision == null || precision < 0) {
            return qty;
        }
        return qty.setScale(precision, java.math.RoundingMode.DOWN).stripTrailingZeros();
    }

    private BigDecimal getMergedPrice(String symbol) {
        String uri = "/market/detail/merged?symbol=" + encodeQuery(symbol.toLowerCase());
        LOG.info("htx GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = publicGet(uri);
        JsonNode tick = requireOk(response, "merged price").path("tick");
        return toDecimal(tick.get("close"));
    }

    private long getServerTimestamp() {
        long now = System.currentTimeMillis();
        long age = now - lastTimeSyncMillis;
        if (age > 30_000L) {
            return fetchServerTimestamp();
        }
        return now + timeOffsetMillis;
    }

    private synchronized long fetchServerTimestamp() {
        long now = System.currentTimeMillis();
        if (now - lastTimeSyncMillis <= 30_000L) {
            return now + timeOffsetMillis;
        }
        try {
            JsonNode resp = publicGet("/v1/common/timestamp");
            JsonNode data = resp == null ? null : resp.get("data");
            if (data != null && data.isNumber()) {
                long serverTime = data.asLong();
                timeOffsetMillis = serverTime - now;
                lastTimeSyncMillis = now;
                LOG.info("htx server time sync: server={} local={} offsetMs={}",
                        serverTime, now, timeOffsetMillis);
                return serverTime;
            }
        } catch (Exception e) {
            LOG.warn("htx server time sync failed: {}", e.getMessage());
        }
        lastTimeSyncMillis = now;
        timeOffsetMillis = 0L;
        return now;
    }

    private JsonNode signedGet(String path, Map<String, String> params, String apiKey, String apiSecret) {
        String signHost = getHost();
        LOG.info("htx signHost={} requestHost={}", signHost, getRequestHost());
        String uri = buildSignedUri("GET", path, params, apiKey, apiSecret, null);
        LOG.info("htx GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = getJson(webClient, uri, false, null, signHost);
        logFailedAttempt("GET", uri, response, "primary");
        if (isSignatureError(response)) {
            String retryHost = getHostWithPort();
            LOG.info("htx signHost={} requestHost={}", retryHost, getRequestHost());
            String retryUri = buildSignedUri("GET", path, params, apiKey, apiSecret, retryHost);
            if (!retryUri.equals(uri)) {
                LOG.info("htx GET {}", LogSanitizer.sanitize(retryUri));
                response = getJson(webClient, retryUri, false, null, retryHost);
                logFailedAttempt("GET", retryUri, response, "host+port");
            }
        }
        if (isSignatureError(response)) {
            String rawUri = buildSignedUriRaw("GET", path, params, apiKey, apiSecret, null);
            LOG.info("htx GET {}", LogSanitizer.sanitize(rawUri));
            response = getJson(webClient, rawUri, false, null, signHost);
            logFailedAttempt("GET", rawUri, response, "raw");
        }
        if (isSignatureError(response) && getAltBaseUrl() != null) {
            WebClient alt = getAltWebClient();
            String altHost = getAltHost();
            LOG.info("htx retry on altBaseUrl={} signHost={}", getAltBaseUrl(), altHost);
            String altUri = buildSignedUri("GET", path, params, apiKey, apiSecret, altHost);
            LOG.info("htx GET {}", LogSanitizer.sanitize(altUri));
            response = getJson(alt, altUri, false, null, altHost);
            logFailedAttempt("GET", altUri, response, "altBaseUrl");
        }
        return response;
    }

    private JsonNode signedGetAuthOnly(String path, Map<String, String> params, String apiKey, String apiSecret) {
        String signHost = getHost();
        LOG.info("htx signHost={} requestHost={}", signHost, getRequestHost());
        String uri = buildSignedUriAuthOnly("GET", path, params, apiKey, apiSecret, null);
        LOG.info("htx GET {}", LogSanitizer.sanitize(uri));
        JsonNode response = getJson(webClient, uri, false, null, signHost);
        logFailedAttempt("GET", uri, response, "primary");
        if (isSignatureError(response)) {
            String retryHost = getHostWithPort();
            LOG.info("htx signHost={} requestHost={}", retryHost, getRequestHost());
            String retryUri = buildSignedUriAuthOnly("GET", path, params, apiKey, apiSecret, retryHost);
            if (!retryUri.equals(uri)) {
                LOG.info("htx GET {}", LogSanitizer.sanitize(retryUri));
                response = getJson(webClient, retryUri, false, null, retryHost);
                logFailedAttempt("GET", retryUri, response, "host+port");
            }
        }
        if (isSignatureError(response)) {
            String rawUri = buildSignedUriAuthOnlyRaw("GET", path, params, apiKey, apiSecret, null);
            LOG.info("htx GET {}", LogSanitizer.sanitize(rawUri));
            response = getJson(webClient, rawUri, false, null, signHost);
            logFailedAttempt("GET", rawUri, response, "raw");
        }
        if (isSignatureError(response) && getAltBaseUrl() != null) {
            WebClient alt = getAltWebClient();
            String altHost = getAltHost();
            LOG.info("htx retry on altBaseUrl={} signHost={}", getAltBaseUrl(), altHost);
            String altUri = buildSignedUriAuthOnly("GET", path, params, apiKey, apiSecret, altHost);
            LOG.info("htx GET {}", LogSanitizer.sanitize(altUri));
            response = getJson(alt, altUri, false, null, altHost);
            logFailedAttempt("GET", altUri, response, "altBaseUrl");
        }
        return response;
    }

    private JsonNode signedPost(String path, Map<String, String> params, Object body, String apiKey, String apiSecret) {
        String signHost = getHost();
        LOG.info("htx signHost={} requestHost={}", signHost, getRequestHost());
        String uri = buildSignedUri("POST", path, params, apiKey, apiSecret, null);
        LOG.info("htx POST {}", LogSanitizer.sanitize(uri));
        JsonNode response = getJson(webClient, uri, true, body, signHost);
        logFailedAttempt("POST", uri, response, "primary");
        if (isSignatureError(response)) {
            String retryHost = getHostWithPort();
            LOG.info("htx signHost={} requestHost={}", retryHost, getRequestHost());
            String retryUri = buildSignedUri("POST", path, params, apiKey, apiSecret, retryHost);
            if (!retryUri.equals(uri)) {
                LOG.info("htx POST {}", LogSanitizer.sanitize(retryUri));
                response = getJson(webClient, retryUri, true, body, retryHost);
                logFailedAttempt("POST", retryUri, response, "host+port");
            }
        }
        if (isSignatureError(response)) {
            String rawUri = buildSignedUriRaw("POST", path, params, apiKey, apiSecret, null);
            LOG.info("htx POST {}", LogSanitizer.sanitize(rawUri));
            response = getJson(webClient, rawUri, true, body, signHost);
            logFailedAttempt("POST", rawUri, response, "raw");
        }
        if (isSignatureError(response) && getAltBaseUrl() != null) {
            WebClient alt = getAltWebClient();
            String altHost = getAltHost();
            LOG.info("htx retry on altBaseUrl={} signHost={}", getAltBaseUrl(), altHost);
            String altUri = buildSignedUri("POST", path, params, apiKey, apiSecret, altHost);
            LOG.info("htx POST {}", LogSanitizer.sanitize(altUri));
            response = getJson(alt, altUri, true, body, altHost);
            logFailedAttempt("POST", altUri, response, "altBaseUrl");
        }
        return response;
    }

    private JsonNode publicGet(String uri) {
        return getJson(webClient, uri, false, null, null);
    }

    private JsonNode getJson(WebClient client, String uri, boolean isPost, Object body, String hostHeader) {
        try {
            URI requestUri = resolveRequestUri(uri, hostHeader);
            String response;
            if (isPost) {
                var request = client.post()
                        .uri(requestUri)
                        .header(HttpHeaders.USER_AGENT, "crypto-console")
                        .header(HttpHeaders.CONTENT_TYPE, "application/json");
                if (StringUtils.isNotBlank(hostHeader)) {
                    request = request.header(HttpHeaders.HOST, hostHeader);
                }
                response = request
                        .bodyValue(body == null ? Collections.emptyMap() : body)
                        .retrieve()
                        .bodyToMono(String.class)
                        .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                            String msg = "HTX request failed: HTTP " + ex.getStatusCode().value();
                            String respBody = ex.getResponseBodyAsString();
                            if (respBody != null && !respBody.isBlank()) {
                                msg = msg + " body=" + respBody;
                            }
                            return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                        })
                        .block();
            } else {
                var request = client.get()
                        .uri(requestUri)
                        .header(HttpHeaders.USER_AGENT, "crypto-console")
                        .header(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
                if (StringUtils.isNotBlank(hostHeader)) {
                    request = request.header(HttpHeaders.HOST, hostHeader);
                }
                response = request
                        .retrieve()
                        .bodyToMono(String.class)
                        .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                            String msg = "HTX request failed: HTTP " + ex.getStatusCode().value();
                            String respBody = ex.getResponseBodyAsString();
                            if (respBody != null && !respBody.isBlank()) {
                                msg = msg + " body=" + respBody;
                            }
                            return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                        })
                        .block();
            }
            if (StringUtils.isBlank(response)) {
                throw new ExchangeException("Empty response from HTX");
            }
            return MAPPER.readTree(response);
        } catch (ExchangeException e) {
            throw e;
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse HTX response", e);
        }
    }

    private URI resolveRequestUri(String uri, String hostHeader) {
        if (StringUtils.startsWithIgnoreCase(uri, "http://") || StringUtils.startsWithIgnoreCase(uri, "https://")) {
            return URI.create(uri);
        }
        URI base = URI.create(baseUrl);
        String scheme = StringUtils.defaultIfBlank(base.getScheme(), "https");
        String host = StringUtils.defaultIfBlank(hostHeader, getRequestHost());
        String pathAndQuery = uri.startsWith("/") ? uri : "/" + uri;
        return URI.create(scheme + "://" + host + pathAndQuery);
    }

    private WebClient getAltWebClient() {
        if (altWebClient != null) {
            return altWebClient;
        }
        synchronized (this) {
            if (altWebClient != null) {
                return altWebClient;
            }
            String url = getAltBaseUrl();
            ExchangeStrategies strategies = ExchangeStrategies.builder()
                    .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                    .build();
            altWebClient = WebClient.builder()
                    .baseUrl(url)
                    .exchangeStrategies(strategies)
                    .build();
            return altWebClient;
        }
    }

    private String getAltBaseUrl() {
        if (altBaseUrl != null) {
            return altBaseUrl;
        }
        String host = getHost();
        if (host.contains("huobi.pro")) {
            altBaseUrl = "https://api.htx.com";
        }
        return altBaseUrl;
    }

    private String getAltHost() {
        if (getAltBaseUrl() == null) {
            return null;
        }
        URI uri = URI.create(getAltBaseUrl());
        String host = uri.getHost();
        return host == null ? null : host.toLowerCase();
    }

    private JsonNode requireOk(JsonNode response, String context) {
        if (response == null) {
            throw new ExchangeException("Unexpected response from HTX " + context + " API");
        }
        if (response.hasNonNull("status")) {
            String status = response.get("status").asText();
            if (!"ok".equalsIgnoreCase(status)) {
                String errCode = response.hasNonNull("err-code") ? response.get("err-code").asText() : "";
                String errMsg = response.hasNonNull("err-msg") ? response.get("err-msg").asText() : "";
                throw new ExchangeException("HTX " + context + " failed: " + errCode + " " + errMsg);
            }
        }
        if (response.hasNonNull("code")) {
            int code = response.get("code").asInt();
            if (code != 200) {
                String message = response.hasNonNull("message") ? response.get("message").asText() : "";
                throw new ExchangeException("HTX " + context + " failed: code=" + code + " " + message);
            }
        }
        return response;
    }

    private JsonNode extractData(JsonNode response, String context) {
        JsonNode resp = requireOk(response, context);
        JsonNode data = resp.get("data");
        return data == null ? com.fasterxml.jackson.databind.node.MissingNode.getInstance() : data;
    }

    private String buildSignedUri(String method, String path, Map<String, String> params, String apiKey, String apiSecret,
                                  String hostOverride) {
        Map<String, String> signedParams = new TreeMap<>();
        signedParams.put("AccessKeyId", apiKey);
        signedParams.put("SignatureMethod", "HmacSHA256");
        signedParams.put("SignatureVersion", "2");
        signedParams.put("Timestamp", TS_FORMAT.format(Instant.ofEpochMilli(getServerTimestamp())));
        if (params != null) {
            signedParams.putAll(params);
        }
        String encodedQuery = buildQueryString(signedParams);
        String signature = sign(method, path, encodedQuery, apiSecret, hostOverride);
        String signHost = StringUtils.isNotBlank(hostOverride) ? hostOverride : getHost();
        LOG.info("htx signString=\n{}", sanitizeCanonical(method, signHost, path, encodedQuery));
        return path + "?" + encodedQuery + "&Signature=" + encodeQuery(signature);
    }

    private String buildHuobiSignedGet(String path, String paramKey, String paramValue, String apiKey, String apiSecret) {
        HuobiUrlParamsBuilder builder = HuobiUrlParamsBuilder.build();
        if (StringUtils.isNotBlank(paramKey) && StringUtils.isNotBlank(paramValue)) {
            builder.putToUrl(paramKey, paramValue);
        }
        new HuobiApiSignature().createSignature(apiKey, apiSecret, "GET", getHost(), path, builder);
        String canonicalQuery = builder.buildSignature();
        LOG.info("htx signString=\n{}", sanitizeCanonical("GET", getHost(), path, canonicalQuery));
        return path + builder.buildUrl();
    }

    private String buildSignedUriAuthOnly(String method, String path, Map<String, String> params, String apiKey, String apiSecret,
                                          String hostOverride) {
        Map<String, String> signedParams = new TreeMap<>();
        signedParams.put("AccessKeyId", apiKey);
        signedParams.put("SignatureMethod", "HmacSHA256");
        signedParams.put("SignatureVersion", "2");
        signedParams.put("Timestamp", TS_FORMAT.format(Instant.ofEpochMilli(getServerTimestamp())));
        String authQuery = buildQueryString(signedParams);
        String signature = sign(method, path, authQuery, apiSecret, hostOverride);
        String signHost = StringUtils.isNotBlank(hostOverride) ? hostOverride : getHost();
        LOG.info("htx signString=\n{}", sanitizeCanonical(method, signHost, path, authQuery));
        String fullQuery = params == null || params.isEmpty()
                ? authQuery
                : authQuery + "&" + buildQueryString(params);
        return path + "?" + fullQuery + "&Signature=" + encodeQuery(signature);
    }

    private String buildSignedUriAuthOnlyRaw(String method, String path, Map<String, String> params, String apiKey, String apiSecret,
                                             String hostOverride) {
        Map<String, String> signedParams = new TreeMap<>();
        signedParams.put("AccessKeyId", apiKey);
        signedParams.put("SignatureMethod", "HmacSHA256");
        signedParams.put("SignatureVersion", "2");
        signedParams.put("Timestamp", TS_FORMAT.format(Instant.ofEpochMilli(getServerTimestamp())));
        String rawAuthQuery = buildRawQueryString(signedParams);
        String signature = sign(method, path, rawAuthQuery, apiSecret, hostOverride);
        String signHost = StringUtils.isNotBlank(hostOverride) ? hostOverride : getHost();
        LOG.info("htx signString=\n{}", sanitizeCanonical(method, signHost, path, rawAuthQuery));
        String fullQuery = params == null || params.isEmpty()
                ? rawAuthQuery
                : rawAuthQuery + "&" + buildQueryString(params);
        return path + "?" + fullQuery + "&Signature=" + encodeQuery(signature);
    }

    private String buildSignedUriRaw(String method, String path, Map<String, String> params, String apiKey, String apiSecret,
                                     String hostOverride) {
        Map<String, String> signedParams = new TreeMap<>();
        signedParams.put("AccessKeyId", apiKey);
        signedParams.put("SignatureMethod", "HmacSHA256");
        signedParams.put("SignatureVersion", "2");
        signedParams.put("Timestamp", TS_FORMAT.format(Instant.ofEpochMilli(getServerTimestamp())));
        if (params != null) {
            signedParams.putAll(params);
        }
        String rawQuery = buildRawQueryString(signedParams);
        String signature = sign(method, path, rawQuery, apiSecret, hostOverride);
        String signHost = StringUtils.isNotBlank(hostOverride) ? hostOverride : getHost();
        LOG.info("htx signString=\n{}", sanitizeCanonical(method, signHost, path, rawQuery));
        return path + "?" + rawQuery + "&Signature=" + encodeQuery(signature);
    }

    private String sign(String method, String path, String query, String secret, String hostOverride) {
        try {
            String host = StringUtils.isNotBlank(hostOverride) ? hostOverride : getHost();
            String payload = method.toUpperCase() + "\n" + host + "\n" + path + "\n" + query;
            return hmacSha256Base64(payload, secret);
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign HTX request", e);
        }
    }

    private static String hmacSha256Base64(String payload, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return java.util.Base64.getEncoder().encodeToString(raw);
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign HTX request", e);
        }
    }

    private String buildQueryString(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
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

    private String buildUrlQuery(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
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

    private static final class HuobiApiSignature {
        private static final String ACCESS_KEY_ID = "AccessKeyId";
        private static final String SIGNATURE_METHOD = "SignatureMethod";
        private static final String SIGNATURE_METHOD_VALUE = "HmacSHA256";
        private static final String SIGNATURE_VERSION = "SignatureVersion";
        private static final String SIGNATURE_VERSION_VALUE = "2";
        private static final String TIMESTAMP = "Timestamp";
        private static final String SIGNATURE = "Signature";

        public void createSignature(String accessKey, String secretKey, String method, String host,
                                    String uri, HuobiUrlParamsBuilder builder) {
            if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
                throw new ExchangeException("API key and secret key are required");
            }
            builder.putToUrl(ACCESS_KEY_ID, accessKey)
                    .putToUrl(SIGNATURE_VERSION, SIGNATURE_VERSION_VALUE)
                    .putToUrl(SIGNATURE_METHOD, SIGNATURE_METHOD_VALUE)
                    .putToUrl(TIMESTAMP, HUOBI_TS_FORMAT.format(Instant.now()));

            StringBuilder sb = new StringBuilder(256);
            sb.append(method.toUpperCase()).append('\n')
                    .append(host.toLowerCase()).append('\n')
                    .append(uri).append('\n')
                    .append(builder.buildSignature());

            String actualSign = hmacSha256Base64(sb.toString(), secretKey);
            builder.putToUrl(SIGNATURE, actualSign);
        }
    }

    private static final class HuobiUrlParamsBuilder {
        private final Map<String, String> paramsMap = new LinkedHashMap<>();

        static HuobiUrlParamsBuilder build() {
            return new HuobiUrlParamsBuilder();
        }

        HuobiUrlParamsBuilder putToUrl(String name, String value) {
            if (StringUtils.isBlank(name) || StringUtils.isBlank(value)) {
                return this;
            }
            paramsMap.put(name, value);
            return this;
        }

        String buildUrl() {
            StringBuilder head = new StringBuilder("?");
            return appendUrl(new LinkedHashMap<>(paramsMap), head);
        }

        String buildSignature() {
            StringBuilder head = new StringBuilder();
            return appendUrl(new TreeMap<>(paramsMap), head);
        }

        private String appendUrl(Map<String, String> map, StringBuilder sb) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (!sb.toString().isEmpty()) {
                    sb.append("&");
                }
                sb.append(entry.getKey())
                        .append("=")
                        .append(rfc3986(entry.getValue()));
            }
            return sb.toString();
        }
    }

    private String buildRawQueryString(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (!sb.isEmpty()) {
                sb.append("&");
            }
            sb.append(entry.getKey())
                    .append("=")
                    .append(entry.getValue());
        }
        return sb.toString();
    }


    private String encodeQuery(String value) {
        try {
            return rfc3986(value);
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode HTX query parameter", e);
        }
    }

    private static String rfc3986(String value) {
        String enc = URLEncoder.encode(value, StandardCharsets.UTF_8);
        return enc.replace("+", "%20");
    }

    private String getHost() {
        URI uri = URI.create(baseUrl);
        String host = uri.getHost();
        if (host == null) {
            throw new ExchangeException("Invalid HTX baseUrl: " + baseUrl);
        }
        return host.toLowerCase();
    }

    private String getHostWithPort() {
        URI uri = URI.create(baseUrl);
        String host = uri.getHost();
        if (host == null) {
            throw new ExchangeException("Invalid HTX baseUrl: " + baseUrl);
        }
        int port = uri.getPort();
        if (port <= 0) {
            port = "http".equalsIgnoreCase(uri.getScheme()) ? 80 : 443;
        }
        return host.toLowerCase() + ":" + port;
    }

    private String getRequestHost() {
        URI uri = URI.create(baseUrl);
        String host = uri.getHost();
        if (host == null) {
            return "";
        }
        int port = uri.getPort();
        if (port <= 0) {
            return host.toLowerCase();
        }
        return host.toLowerCase() + ":" + port;
    }

    private boolean isSignatureError(JsonNode response) {
        if (response == null) {
            return false;
        }
        String status = response.hasNonNull("status") ? response.get("status").asText() : null;
        String errCode = response.hasNonNull("err-code") ? response.get("err-code").asText() : null;
        if ("error".equalsIgnoreCase(status) && "api-signature-not-valid".equalsIgnoreCase(errCode)) {
            return true;
        }
        if (response.hasNonNull("code") && response.get("code").asInt() == 1003) {
            return true;
        }
        return false;
    }

    private void logFailedAttempt(String method, String uri, JsonNode response, String attempt) {
        if (response == null) {
            LOG.warn("htx {} {} attempt={} failed: empty response", method, LogSanitizer.sanitize(uri), attempt);
            return;
        }
        if (response.hasNonNull("status") && "ok".equalsIgnoreCase(response.get("status").asText())) {
            return;
        }
        if (response.hasNonNull("code") && response.get("code").asInt() == 200) {
            return;
        }
        String status = response.hasNonNull("status") ? response.get("status").asText() : null;
        String code = response.hasNonNull("err-code") ? response.get("err-code").asText()
                : (response.hasNonNull("code") ? response.get("code").asText() : null);
        String msg = response.hasNonNull("err-msg") ? response.get("err-msg").asText()
                : (response.hasNonNull("message") ? response.get("message").asText() : null);
        LOG.warn("htx {} {} attempt={} failed: status={} code={} msg={}",
                method, LogSanitizer.sanitize(uri), attempt, status, code, msg);
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

    private String sanitizeCanonical(String method, String host, String path, String query) {
        String sanitizedQuery = query.replaceAll("AccessKeyId=[^&]+", "AccessKeyId=***");
        return method.toUpperCase() + "\n" + host + "\n" + path + "\n" + sanitizedQuery;
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

    private record SymbolInfo(Integer amountPrecision, BigDecimal minOrderValue, BigDecimal minOrderAmt,
                              BigDecimal sellMarketMinOrderAmt) {
    }

    private record ChainInfo(String chain, String displayName, boolean depositEnabled, boolean withdrawEnabled,
                             boolean isDefault) {
    }
}
