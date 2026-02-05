package com.crypto.console.exchanges.coinex;

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
public class CoinExClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String API_PREFIX = "/v2";
    private static final String ALGORITHM = "HmacSHA256";

    public CoinExClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("coinex", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for coinex");
        }

        JsonNode response = signedGet("/assets/spot/balance", null, apiKey, apiSecret);
        JsonNode data = requireOk(response, "spot balance").path("data");
        if (data.isArray()) {
            for (JsonNode item : data) {
                String ccy = item.hasNonNull("ccy") ? item.get("ccy").asText() : null;
                if (ccy != null && ccy.equalsIgnoreCase(asset)) {
                    BigDecimal free = toDecimal(item.get("available"));
                    BigDecimal locked = toDecimal(item.get("frozen"));
                    return new Balance(ccy.toUpperCase(), free, locked);
                }
            }
        }

        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /assets/deposit-withdraw-config?ccy= (public)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /spot/depth (public)");
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String market = (base + quote).toUpperCase();
        MarketInfo info = getMarketInfo(market);
        if (info == null || !info.apiTradingAvailable) {
            throw new ExchangeException("Invalid symbol or API trading not available: " + market);
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("market", market);
        params.put("limit", "50");
        params.put("interval", "0");
        String uri = "/spot/depth?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "order book").path("data");
        JsonNode depth = data.get("depth");
        if (depth == null || depth.isNull()) {
            depth = data;
        }
        JsonNode asks = depth.get("asks");
        if (asks == null || !asks.isArray()) {
            throw new ExchangeException("Unexpected response from CoinEx order book API");
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
            throw new ExchangeException("No ask liquidity available for " + market);
        }

        BigDecimal averagePrice = spentQuote.divide(boughtBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(market, quoteAmount, spentQuote, boughtBase, averagePrice, List.copyOf(affectedItems));
    }

    @Override
    public BuyInfoResult sellInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String market = (base + quote).toUpperCase();
        MarketInfo info = getMarketInfo(market);
        if (info == null || !info.apiTradingAvailable) {
            throw new ExchangeException("Invalid symbol or API trading not available: " + market);
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("market", market);
        params.put("limit", "50");
        params.put("interval", "0");
        String uri = "/spot/depth?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "order book").path("data");
        JsonNode depth = data.get("depth");
        if (depth == null || depth.isNull()) {
            depth = data;
        }
        JsonNode bids = depth.get("bids");
        if (bids == null || !bids.isArray()) {
            throw new ExchangeException("Unexpected response from CoinEx order book API");
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
            throw new ExchangeException("No bid liquidity available for " + market);
        }

        BigDecimal averagePrice = receivedQuote.divide(soldBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(market, quoteAmount, receivedQuote, soldBase, averagePrice, List.copyOf(affectedItems));
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
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for coinex");
        }

        DepositWithdrawConfig config = getDepositWithdrawConfig(asset);
        String chain = resolveChain(config, network, true);
        if (StringUtils.isBlank(chain)) {
            throw new ExchangeException("CoinEx does not support withdrawals for asset: " + asset.toUpperCase());
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ccy", config.assetCcy);
        body.put("chain", chain);
        body.put("to_address", address);
        body.put("amount", amount.toPlainString());
        body.put("withdraw_method", "on_chain");
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("memo", memoOrNull);
        }

        JsonNode response = signedPost("/assets/withdraw", body, apiKey, apiSecret);
        JsonNode data = requireOk(response, "withdraw").path("data");
        String id = data.hasNonNull("withdraw_id") ? data.get("withdraw_id").asText() : null;
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from CoinEx withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode response = publicGet("/time");
        JsonNode data = requireOk(response, "time").path("data");
        long serverTime = data.hasNonNull("timestamp") ? data.get("timestamp").asLong() : System.currentTimeMillis();
        long offset = serverTime - System.currentTimeMillis();
        return new ExchangeTime(serverTime, offset);
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        DepositWithdrawConfig config = getDepositWithdrawConfig(asset);
        Set<String> networks = new HashSet<>();
        for (ChainConfig chain : config.chains) {
            if (chain.depositEnabled && StringUtils.isNotBlank(chain.chain)) {
                networks.add(chain.chain.trim().toUpperCase());
            }
        }
        if (networks.isEmpty()) {
            throw new ExchangeException("CoinEx does not support deposits for asset: " + asset.toUpperCase());
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
            throw new ExchangeException("Missing API credentials for coinex");
        }

        DepositWithdrawConfig config = getDepositWithdrawConfig(asset);
        String chain = resolveChain(config, network, false);
        if (StringUtils.isBlank(chain)) {
            throw new ExchangeException("CoinEx does not support deposits for asset: " + asset.toUpperCase());
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("ccy", config.assetCcy);
        params.put("chain", chain);
        JsonNode response = signedGet("/assets/deposit-address", params, apiKey, apiSecret);
        JsonNode data = requireOk(response, "deposit address").path("data");
        JsonNode addressNode = data.get("address");
        return addressNode == null ? null : addressNode.asText();
    }

    @Override
    public ExchangeCapabilities capabilities() {
        return new ExchangeCapabilities(true, false, false, true, true, true, true);
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
            case "SOLANA", "SOL", "SOLANASOL" -> "SOL";
            case "TRON", "TRC20", "TRX" -> "TRC20";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "COINEXSMARTCHAIN", "CSC" -> "CSC";
            case "KAIA" -> "KAIA";
            case "CELO" -> "CELO";
            case "HECO" -> "HECO";
            case "PLASMA" -> "PLASMA";
            case "KCC", "KUCOINCOMMUNITYCHAIN" -> "KCC";
            default -> cleaned;
        };
    }

    private OrderResult submitMarketOrder(OrderSide side, String base, String quote, BigDecimal amount, boolean isQuoteAmount) {
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for coinex");
        }

        String market = (base + quote).toUpperCase();
        MarketInfo info = getMarketInfo(market);
        if (info == null || !info.apiTradingAvailable) {
            throw new ExchangeException("Invalid symbol or API trading not available: " + market);
        }

        if (isQuoteAmount) {
            if (info.quotePrecision != null && info.quotePrecision >= 0) {
                amount = amount.setScale(info.quotePrecision, java.math.RoundingMode.DOWN).stripTrailingZeros();
            }
        } else {
            BigDecimal qty = applyLotSize(info, amount);
            if (qty.signum() <= 0) {
                throw new ExchangeException("Sell quantity below minimum lot size for " + market);
            }
            BigDecimal minAmount = info.minAmount;
            if (minAmount != null && minAmount.signum() > 0) {
                BigDecimal price = getLatestPrice(market);
                if (price != null && price.signum() > 0) {
                    BigDecimal notional = qty.multiply(price);
                    BigDecimal minNotional = minAmount.multiply(price);
                    if (notional.compareTo(minNotional) < 0) {
                        throw new ExchangeException("Order value " + notional + " below min notional " + minNotional + " for " + market);
                    }
                }
            }
            amount = qty;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("market", market);
        body.put("market_type", "SPOT");
        body.put("side", side == OrderSide.BUY ? "buy" : "sell");
        body.put("type", "market");
        body.put("amount", amount.toPlainString());
        body.put("ccy", isQuoteAmount ? quote.toUpperCase() : base.toUpperCase());

        JsonNode response = signedPost("/spot/order", body, apiKey, apiSecret);
        JsonNode data = requireOk(response, "order").path("data");
        String orderId = data.hasNonNull("order_id") ? data.get("order_id").asText() : null;
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from CoinEx order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market order submitted");
    }

    private MarketInfo getMarketInfo(String market) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("market", market);
        String uri = "/spot/market?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "market info").path("data");
        if (!data.isArray() || data.isEmpty()) {
            return null;
        }
        JsonNode item = data.get(0);
        BigDecimal minAmount = toDecimal(item.get("min_amount"));
        Integer basePrecision = item.hasNonNull("base_ccy_precision") ? item.get("base_ccy_precision").asInt() : null;
        Integer quotePrecision = item.hasNonNull("quote_ccy_precision") ? item.get("quote_ccy_precision").asInt() : null;
        boolean apiTradingAvailable = item.hasNonNull("is_api_trading_available") && item.get("is_api_trading_available").asBoolean();
        return new MarketInfo(minAmount, basePrecision, quotePrecision, apiTradingAvailable);
    }

    private BigDecimal getLatestPrice(String market) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("market", market);
        String uri = "/spot/ticker?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "ticker").path("data");
        if (data.isArray() && !data.isEmpty()) {
            return toDecimal(data.get(0).get("last"));
        }
        return null;
    }

    private BigDecimal applyLotSize(MarketInfo info, BigDecimal quantity) {
        if (info == null) {
            return quantity;
        }
        BigDecimal min = info.minAmount;
        Integer precision = info.basePrecision;
        if (precision != null && precision >= 0) {
            quantity = quantity.setScale(precision, java.math.RoundingMode.DOWN);
        }
        if (min != null && min.signum() > 0 && quantity.compareTo(min) < 0) {
            return BigDecimal.ZERO;
        }
        return quantity.stripTrailingZeros();
    }

    private DepositWithdrawConfig getDepositWithdrawConfig(String asset) {
        String assetUpper = asset.toUpperCase();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("ccy", assetUpper);
        String uri = "/assets/deposit-withdraw-config?" + buildQueryString(params);
        JsonNode response = publicGet(uri);
        JsonNode data = requireOk(response, "deposit/withdraw config").path("data");
        if (data == null || data.isNull()) {
            throw new ExchangeException("CoinEx does not support asset: " + assetUpper);
        }
        JsonNode assetNode = data.get("asset");
        JsonNode chainsNode = data.get("chains");
        String ccy = assetNode != null && assetNode.hasNonNull("ccy") ? assetNode.get("ccy").asText() : assetUpper;
        boolean depositEnabled = assetNode != null && assetNode.hasNonNull("deposit_enabled") && assetNode.get("deposit_enabled").asBoolean(false);
        boolean withdrawEnabled = assetNode != null && assetNode.hasNonNull("withdraw_enabled") && assetNode.get("withdraw_enabled").asBoolean(false);
        List<ChainConfig> chains = new ArrayList<>();
        if (chainsNode != null && chainsNode.isArray()) {
            for (JsonNode chain : chainsNode) {
                String chainName = chain.hasNonNull("chain") ? chain.get("chain").asText() : null;
                boolean chainDepositEnabled = chain.hasNonNull("deposit_enabled") && chain.get("deposit_enabled").asBoolean(false);
                boolean chainWithdrawEnabled = chain.hasNonNull("withdraw_enabled") && chain.get("withdraw_enabled").asBoolean(false);
                chains.add(new ChainConfig(chainName, chainDepositEnabled, chainWithdrawEnabled));
            }
        }
        if (chains.isEmpty()) {
            throw new ExchangeException("CoinEx does not support asset: " + assetUpper);
        }
        return new DepositWithdrawConfig(ccy, depositEnabled, withdrawEnabled, chains);
    }

    private String resolveChain(DepositWithdrawConfig config, String network, boolean forWithdraw) {
        if (config == null) {
            return null;
        }
        if (forWithdraw && !config.withdrawEnabled) {
            return null;
        }
        if (!forWithdraw && !config.depositEnabled) {
            return null;
        }
        List<ChainConfig> candidates = new ArrayList<>();
        for (ChainConfig chain : config.chains) {
            if (forWithdraw && !chain.withdrawEnabled) {
                continue;
            }
            if (!forWithdraw && !chain.depositEnabled) {
                continue;
            }
            candidates.add(chain);
        }
        if (candidates.isEmpty()) {
            return null;
        }
        String normalized = normalizeDepositNetwork(network);
        if (StringUtils.isBlank(normalized)) {
            if (candidates.size() == 1) {
                return normalizeChainParam(candidates.get(0).chain);
            }
            StringBuilder available = new StringBuilder();
            for (ChainConfig chain : candidates) {
                if (StringUtils.isBlank(chain.chain)) {
                    continue;
                }
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(chain.chain);
            }
            throw new ExchangeException("Network is required for " + config.assetCcy + " (available: " + available + ")");
        }
        for (ChainConfig chain : candidates) {
            String chainName = chain.chain;
            if (StringUtils.isBlank(chainName)) {
                continue;
            }
            String normalizedChain = normalizeDepositNetwork(chainName);
            if (normalized.equalsIgnoreCase(normalizedChain)) {
                return normalizeChainParam(chainName);
            }
        }
        throw new ExchangeException("CoinEx does not support network " + network + " for asset " + config.assetCcy.toUpperCase());
    }

    private String normalizeChainParam(String chain) {
        if (StringUtils.isBlank(chain)) {
            return chain;
        }
        int openIdx = chain.indexOf('(');
        int closeIdx = chain.indexOf(')');
        if (openIdx >= 0 && closeIdx > openIdx) {
            String inner = chain.substring(openIdx + 1, closeIdx);
            if (StringUtils.isNotBlank(inner)) {
                return inner;
            }
        }
        return chain;
    }

    private JsonNode signedGet(String path, Map<String, String> params, String apiKey, String apiSecret) {
        String query = params == null ? "" : buildQueryString(params);
        String uri = query.isEmpty() ? API_PREFIX + path : API_PREFIX + path + "?" + query;
        String requestPath = query.isEmpty() ? API_PREFIX + path : API_PREFIX + path + "?" + query;
        String timestamp = String.valueOf(System.currentTimeMillis());
        String signature = sign("GET", requestPath, "", timestamp, apiSecret);
        LOG.info("coinex GET {}", LogSanitizer.sanitize(uri));
        String body = webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header("X-COINEX-KEY", apiKey)
                .header("X-COINEX-SIGN", signature)
                .header("X-COINEX-TIMESTAMP", timestamp)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "CoinEx request failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        return parseJson(body, "CoinEx");
    }

    private JsonNode signedPost(String path, Map<String, Object> body, String apiKey, String apiSecret) {
        String jsonBody = "";
        try {
            if (body != null) {
                jsonBody = MAPPER.writeValueAsString(body);
            }
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize CoinEx request body", e);
        }
        String requestPath = API_PREFIX + path;
        String timestamp = String.valueOf(System.currentTimeMillis());
        String signature = sign("POST", requestPath, jsonBody, timestamp, apiSecret);
        String uri = API_PREFIX + path;
        LOG.info("coinex POST {}", LogSanitizer.sanitize(uri));
        String respBody = webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header("X-COINEX-KEY", apiKey)
                .header("X-COINEX-SIGN", signature)
                .header("X-COINEX-TIMESTAMP", timestamp)
                .bodyValue(jsonBody)
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "CoinEx request failed: HTTP " + ex.getStatusCode().value();
                    String bodyText = ex.getResponseBodyAsString();
                    if (bodyText != null && !bodyText.isBlank()) {
                        msg = msg + " body=" + bodyText;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        return parseJson(respBody, "CoinEx");
    }

    private JsonNode publicGet(String uri) {
        String full = API_PREFIX + uri;
        LOG.info("coinex GET {}", LogSanitizer.sanitize(full));
        String body = webClient.get()
                .uri(full)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String msg = "CoinEx request failed: HTTP " + ex.getStatusCode().value();
                    String respBody = ex.getResponseBodyAsString();
                    if (respBody != null && !respBody.isBlank()) {
                        msg = msg + " body=" + respBody;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
        return parseJson(body, "CoinEx");
    }

    private JsonNode parseJson(String body, String context) {
        if (StringUtils.isBlank(body)) {
            throw new ExchangeException("Empty response from " + context);
        }
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new ExchangeException("Failed to parse " + context + " response", e);
        }
    }

    private JsonNode requireOk(JsonNode response, String context) {
        if (response == null) {
            throw new ExchangeException("Unexpected response from CoinEx " + context + " API");
        }
        if (response.hasNonNull("code")) {
            int code = response.get("code").asInt(-1);
            if (code != 0) {
                String msg = response.hasNonNull("message") ? response.get("message").asText() : "";
                throw new ExchangeException("CoinEx " + context + " failed: code=" + code + " " + msg);
            }
        }
        return response;
    }

    private String sign(String method, String requestPath, String body, String timestamp, String apiSecret) {
        try {
            String payload = method.toUpperCase() + requestPath + (body == null ? "" : body) + timestamp;
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(apiSecret.getBytes(StandardCharsets.ISO_8859_1), ALGORITHM));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.ISO_8859_1));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign CoinEx request", e);
        }
    }

    private String buildQueryString(Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return "";
        }
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
            String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
            return encoded.replace("+", "%20");
        } catch (Exception e) {
            throw new ExchangeException("Failed to encode CoinEx query parameter", e);
        }
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

    private enum OrderSide {
        BUY,
        SELL
    }

    private record MarketInfo(BigDecimal minAmount, Integer basePrecision, Integer quotePrecision, boolean apiTradingAvailable) {
    }

    private record DepositWithdrawConfig(String assetCcy, boolean depositEnabled, boolean withdrawEnabled, List<ChainConfig> chains) {
    }

    private record ChainConfig(String chain, boolean depositEnabled, boolean withdrawEnabled) {
    }
}
