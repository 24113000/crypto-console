package com.crypto.console.exchanges.lbank;

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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class LBankClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final String SIGNATURE_METHOD = "HmacSHA256";

    public LBankClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("lbank", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for lbank");
        }

        JsonNode response = signedPost("/v2/supplement/user_info_account.do", new LinkedHashMap<>(), apiKey, apiSecret);
        JsonNode data = requireSuccess(response, "balance");
        JsonNode balances = data.has("balances") ? data.get("balances") : data;
        if (balances != null && balances.isArray()) {
            for (JsonNode item : balances) {
                String coin = textOf(item, "asset", "coin", "currency");
                if (coin != null && coin.equalsIgnoreCase(asset)) {
                    BigDecimal free = toDecimal(item.get("free"));
                    BigDecimal locked = toDecimal(item.get("locked"));
                    if (locked.signum() == 0) {
                        locked = toDecimal(item.get("freeze"));
                    }
                    return new Balance(coin.toUpperCase(), free, locked);
                }
            }
        }

        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("POST /v2/supplement/user_info.do (signed)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /v2/depth.do (public)");
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String symbol = (base + "_" + quote).toLowerCase();
        String uri = "/v2/depth.do?symbol=" + symbol + "&size=200";
        JsonNode response = publicGet(uri);
        JsonNode data = requireSuccess(response, "depth");
        JsonNode asks = data.has("asks") ? data.get("asks") : response.get("asks");
        if (asks == null || !asks.isArray()) {
            throw new ExchangeException("Unexpected response from LBank depth API");
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

        String symbol = (base + "_" + quote).toLowerCase();
        String uri = "/v2/depth.do?symbol=" + symbol + "&size=200";
        JsonNode response = publicGet(uri);
        JsonNode data = requireSuccess(response, "depth");
        JsonNode bids = data.has("bids") ? data.get("bids") : response.get("bids");
        if (bids == null || !bids.isArray()) {
            throw new ExchangeException("Unexpected response from LBank depth API");
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
        return submitMarketOrder(true, base, quote, quoteAmount);
    }

    @Override
    public OrderResult marketSell(String base, String quote, BigDecimal baseAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (baseAmount == null || baseAmount.signum() <= 0) {
            throw new ExchangeException("Base amount must be positive");
        }
        return submitMarketOrder(false, base, quote, baseAmount);
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
            throw new ExchangeException("Missing API credentials for lbank");
        }

        CoinConfig coin = resolveCoin(asset);
        NetworkConfig networkConfig = resolveNetwork(coin, network, true);
        String fee = networkConfig.withdrawFee;
        if (StringUtils.isBlank(fee)) {
            fee = findWithdrawFeeFromUserInfo(asset, networkConfig.name);
        }
        if (StringUtils.isBlank(fee)) {
            throw new ExchangeException("LBank withdraw requires fee parameter for " + asset.toUpperCase());
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("coin", coin.asset.toLowerCase());
        params.put("amount", amount.toPlainString());
        params.put("address", address);
        params.put("fee", fee);
        params.put("networkName", normalizeNetworkParam(networkConfig.name));
        if (StringUtils.isNotBlank(memoOrNull)) {
            params.put("memo", memoOrNull);
        }

        JsonNode response = signedPost("/v2/spot/wallet/withdraw.do", params, apiKey, apiSecret);
        JsonNode data = requireSuccess(response, "withdraw");
        String id = textOf(data, "withdraw_id", "withdrawId", "id");
        if (StringUtils.isBlank(id) && data.isTextual()) {
            id = data.asText();
        }
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from LBank withdraw API");
        }
        return new WithdrawResult(id, "SUBMITTED", "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        long serverTime = getServerTime();
        long offset = serverTime - System.currentTimeMillis();
        return new ExchangeTime(serverTime, offset);
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        CoinConfig coin = resolveCoin(asset);
        Set<String> networks = new HashSet<>();
        for (NetworkConfig network : coin.networks) {
            if (network.depositEnabled && StringUtils.isNotBlank(network.name)) {
                networks.add(network.name.trim().toUpperCase());
            }
        }
        if (networks.isEmpty()) {
            throw new ExchangeException("LBank does not support deposits for asset: " + asset.toUpperCase());
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
            throw new ExchangeException("Missing API credentials for lbank");
        }

        CoinConfig coin = resolveCoin(asset);
        NetworkConfig networkConfig = resolveNetwork(coin, network, false);

        Map<String, String> params = new LinkedHashMap<>();
        params.put("coin", coin.asset.toLowerCase());
        params.put("networkName", normalizeNetworkParam(networkConfig.name));
        JsonNode response = signedPost("/v2/supplement/get_deposit_address.do", params, apiKey, apiSecret);
        JsonNode data = requireSuccess(response, "deposit address");
        String address = textOf(data, "address", "addr", "depositAddress");
        if (StringUtils.isBlank(address)) {
            return null;
        }
        return address;
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
            case "ETHEREUM", "ERC20", "ETH" -> "ERC20";
            case "POLYGON", "MATIC" -> "MATIC";
            case "SOLANA", "SOL", "SOLANASOL" -> "SOL";
            case "TRON", "TRC20", "TRX", "TRONTRC20" -> "TRC20";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "BASE" -> "BASE";
            default -> cleaned;
        };
    }

    private OrderResult submitMarketOrder(boolean isBuy, String base, String quote, BigDecimal amount) {
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for lbank");
        }

        String symbol = (base + "_" + quote).toLowerCase();
        SymbolInfo info = getSymbolInfo(symbol);
        if (info != null && info.quantityAccuracy != null && !isBuy) {
            amount = applyLotSize(amount, info.quantityAccuracy);
            if (amount.signum() <= 0) {
                throw new ExchangeException("Sell quantity below minimum lot size for " + symbol);
            }
            if (info.minTranQty != null && info.minTranQty.signum() > 0 && amount.compareTo(info.minTranQty) < 0) {
                throw new ExchangeException("Sell quantity " + amount + " below min quantity " + info.minTranQty + " for " + symbol);
            }
            BigDecimal price = getLatestPrice(symbol);
            if (price != null && price.signum() > 0 && info.minTranQty != null && info.minTranQty.signum() > 0) {
                BigDecimal minNotional = info.minTranQty.multiply(price);
                BigDecimal notional = amount.multiply(price);
                if (notional.compareTo(minNotional) < 0) {
                    throw new ExchangeException("Order value " + notional + " below min notional " + minNotional + " for " + symbol);
                }
            }
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        if (isBuy) {
            params.put("type", "buy_market");
            params.put("price", amount.toPlainString());
        } else {
            params.put("type", "sell_market");
            params.put("amount", amount.toPlainString());
        }

        JsonNode response = signedPost("/v2/supplement/create_order.do", params, apiKey, apiSecret);
        JsonNode data = requireSuccess(response, "order");
        String orderId = textOf(data, "order_id", "orderId", "id");
        if (StringUtils.isBlank(orderId) && data.isTextual()) {
            orderId = data.asText();
        }
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from LBank order API");
        }
        return new OrderResult(orderId, "SUBMITTED", "market order submitted");
    }

    private SymbolInfo getSymbolInfo(String symbol) {
        String uri = "/v2/accuracy.do";
        JsonNode response = publicGet(uri);
        JsonNode data = requireSuccess(response, "symbol accuracy");
        JsonNode list = data.has("data") ? data.get("data") : data;
        if (list != null && list.isArray()) {
            for (JsonNode item : list) {
                String itemSymbol = textOf(item, "symbol");
                if (itemSymbol != null && itemSymbol.equalsIgnoreCase(symbol)) {
                    Integer quantityAccuracy = item.hasNonNull("quantityAccuracy") ? item.get("quantityAccuracy").asInt() : null;
                    BigDecimal minTranQua = toDecimal(item.get("minTranQua"));
                    return new SymbolInfo(symbol, quantityAccuracy, minTranQua);
                }
            }
        }
        return null;
    }

    private BigDecimal getLatestPrice(String symbol) {
        String uri = "/v2/supplement/ticker/price.do?symbol=" + symbol;
        JsonNode response = publicGet(uri);
        JsonNode data = requireSuccess(response, "price");
        JsonNode node = data.has("data") ? data.get("data") : data;
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.hasNonNull("ticker")) {
            return toDecimal(node.get("ticker"));
        }
        if (node.hasNonNull("price")) {
            return toDecimal(node.get("price"));
        }
        if (node.isTextual() || node.isNumber()) {
            return toDecimal(node);
        }
        return null;
    }

    private BigDecimal applyLotSize(BigDecimal quantity, int quantityAccuracy) {
        if (quantityAccuracy < 0) {
            return quantity;
        }
        BigDecimal step = BigDecimal.ONE.scaleByPowerOfTen(-quantityAccuracy);
        if (step.signum() <= 0) {
            return quantity;
        }
        BigDecimal steps = quantity.divide(step, 0, java.math.RoundingMode.DOWN);
        BigDecimal adjusted = step.multiply(steps);
        return adjusted.stripTrailingZeros();
    }

    private CoinConfig resolveCoin(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        CoinConfig fromAssetConfigs = getCoinConfigFromAssetConfigs(asset);
        if (fromAssetConfigs != null) {
            return fromAssetConfigs;
        }
        List<CoinConfig> coins = getCoinConfigsFromUserInfo();
        for (CoinConfig coin : coins) {
            if (coin.asset != null && coin.asset.equalsIgnoreCase(asset)) {
                return coin;
            }
        }
        throw new ExchangeException("LBank does not support asset: " + asset.toUpperCase());
    }

    private NetworkConfig resolveNetwork(CoinConfig coin, String network, boolean forWithdraw) {
        if (coin == null) {
            throw new ExchangeException("Asset is required");
        }
        List<NetworkConfig> candidates = new ArrayList<>();
        for (NetworkConfig cfg : coin.networks) {
            if (forWithdraw && !cfg.withdrawEnabled) {
                continue;
            }
            if (!forWithdraw && !cfg.depositEnabled) {
                continue;
            }
            candidates.add(cfg);
        }
        if (candidates.isEmpty()) {
            throw new ExchangeException("LBank does not support " + (forWithdraw ? "withdrawals" : "deposits") +
                    " for asset: " + coin.asset.toUpperCase());
        }

        if (StringUtils.isBlank(network)) {
            if (candidates.size() == 1) {
                return candidates.get(0);
            }
            for (NetworkConfig cfg : candidates) {
                if (cfg.isDefault) {
                    return cfg;
                }
            }
            StringBuilder available = new StringBuilder();
            for (NetworkConfig cfg : candidates) {
                if (StringUtils.isBlank(cfg.name)) {
                    continue;
                }
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(cfg.name.toUpperCase());
            }
            throw new ExchangeException("Network is required for " + coin.asset.toUpperCase() +
                    " (available: " + available + ")");
        }

        String normalized = normalizeDepositNetwork(network);
        for (NetworkConfig cfg : candidates) {
            String name = cfg.name;
            if (StringUtils.isBlank(name)) {
                continue;
            }
            String normalizedName = normalizeDepositNetwork(name);
            if (normalized != null && normalized.equalsIgnoreCase(normalizedName)) {
                return cfg;
            }
            if (StringUtils.isNotBlank(cfg.network)) {
                String normalizedNetwork = normalizeDepositNetwork(cfg.network);
                if (normalized != null && normalized.equalsIgnoreCase(normalizedNetwork)) {
                    return cfg;
                }
            }
        }
        throw new ExchangeException("LBank does not support network " + network + " for asset " + coin.asset.toUpperCase());
    }

    private CoinConfig getCoinConfigFromAssetConfigs(String asset) {
        List<NetworkConfig> networks = getAssetNetworks(asset);
        if (networks == null || networks.isEmpty()) {
            return null;
        }
        return new CoinConfig(asset.toUpperCase(), networks);
    }

    private List<NetworkConfig> getAssetNetworks(String asset) {
        String assetCode = asset == null ? null : asset.toLowerCase();
        String uri = "/v2/assetConfigs.do?assetCode=" + assetCode;
        JsonNode response = publicGet(uri);
        JsonNode data = requireSuccess(response, "asset configs");
        JsonNode list = data.has("data") ? data.get("data") : data;
        if (list != null && list.has("data") && list.get("data").isArray()) {
            list = list.get("data");
        }
        if (list != null && list.has("assetConfigs") && list.get("assetConfigs").isArray()) {
            list = list.get("assetConfigs");
        }
        if (list == null || !list.isArray()) {
            return List.of();
        }

        List<NetworkConfig> networks = new ArrayList<>();
        for (JsonNode item : list) {
            String assetCodeItem = textOf(item, "assetCode", "asset");
            if (assetCodeItem == null || !assetCodeItem.equalsIgnoreCase(asset)) {
                continue;
            }
            String chainName = textOf(item, "chainName", "chain");
            boolean canDeposit = boolOf(item, "canDeposit", "canRecharge");
            boolean canDraw = boolOf(item, "canDraw", "canWithdraw");
            boolean hasMemo = boolOf(item, "hasMemo");
            JsonNode feeNode = item.get("assetFee");
            String fee = feeNode == null ? null : textOf(feeNode, "feeAmt", "fee");
            boolean isDefault = boolOf(item, "isDefault");
            networks.add(new NetworkConfig(chainName, chainName, canDeposit, canDraw, fee, isDefault, hasMemo));
        }
        return networks;
    }

    private List<CoinConfig> getCoinConfigsFromUserInfo() {
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for lbank");
        }
        JsonNode response = signedPost("/v2/supplement/user_info.do", new LinkedHashMap<>(), apiKey, apiSecret);
        JsonNode data = requireSuccess(response, "coin config");
        JsonNode list = null;
        if (data.isArray()) {
            list = data;
        } else if (data.has("data")) {
            JsonNode nested = data.get("data");
            if (nested != null && nested.isArray()) {
                list = nested;
            } else if (nested != null && nested.has("assetConfigs") && nested.get("assetConfigs").isArray()) {
                list = nested.get("assetConfigs");
            } else if (nested != null && nested.has("data") && nested.get("data").isArray()) {
                list = nested.get("data");
            }
        } else if (data.has("asset")) {
            list = data;
        }
        if (list == null || !list.isArray()) {
            throw new ExchangeException("Unexpected response from LBank coin config API");
        }

        List<CoinConfig> coins = new ArrayList<>();
        for (JsonNode item : list) {
            String asset = textOf(item, "asset", "coin", "currency");
            if (StringUtils.isBlank(asset)) {
                continue;
            }
            List<NetworkConfig> networks = new ArrayList<>();
            JsonNode networkList = item.get("networkList");
            if (networkList != null && networkList.isArray()) {
                for (JsonNode net : networkList) {
                    String name = textOf(net, "name", "networkName", "chain");
                    String network = textOf(net, "network");
                    boolean depositEnabled = boolOf(net, "depositEnable", "depositEnabled");
                    boolean withdrawEnabled = boolOf(net, "withdrawEnable", "withdrawEnabled");
                    String fee = textOf(net, "withdrawFee", "withdrawFeeRate", "fee");
                    boolean isDefault = boolOf(net, "isDefault");
                    boolean memoRequired = boolOf(net, "memoRequired", "memo", "tagRequired");
                    networks.add(new NetworkConfig(name, network, depositEnabled, withdrawEnabled, fee, isDefault, memoRequired));
                }
            }
            coins.add(new CoinConfig(asset, networks));
        }

        return coins;
    }

    private String findWithdrawFeeFromUserInfo(String asset, String networkName) {
        List<CoinConfig> coins = getCoinConfigsFromUserInfo();
        String normalized = normalizeDepositNetwork(networkName);
        for (CoinConfig coin : coins) {
            if (coin.asset == null || !coin.asset.equalsIgnoreCase(asset)) {
                continue;
            }
            for (NetworkConfig cfg : coin.networks) {
                if (StringUtils.isBlank(cfg.name)) {
                    continue;
                }
                String normalizedName = normalizeDepositNetwork(cfg.name);
                if (normalized != null && normalized.equalsIgnoreCase(normalizedName)) {
                    return cfg.withdrawFee;
                }
            }
        }
        return null;
    }

    private JsonNode signedPost(String path, Map<String, String> params, String apiKey, String apiSecret) {
        SignedPayload payload = buildSignedPayload(params, apiKey, apiSecret);
        String uri = path + "?" + payload.body;
        LOG.info("lbank POST {}", LogSanitizer.sanitize(uri));
        return webClient.post()
                .uri(path)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header("timestamp", payload.timestamp)
                .header("signature_method", SIGNATURE_METHOD)
                .header("echostr", payload.echostr)
                .bodyValue(payload.body)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String bodyText = ex.getResponseBodyAsString();
                    String msg = "LBank request failed: HTTP " + ex.getStatusCode().value();
                    if (bodyText != null && !bodyText.isBlank()) {
                        msg = msg + " body=" + bodyText;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode publicGet(String uri) {
        LOG.info("lbank GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String bodyText = ex.getResponseBodyAsString();
                    String msg = "LBank request failed: HTTP " + ex.getStatusCode().value();
                    if (bodyText != null && !bodyText.isBlank()) {
                        msg = msg + " body=" + bodyText;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private SignedPayload buildSignedPayload(Map<String, String> params, String apiKey, String apiSecret) {
        String timestamp = String.valueOf(getServerTime());
        String echostr = randomEchoStr();
        Map<String, String> signParams = new LinkedHashMap<>();
        signParams.put("api_key", apiKey);
        if (params != null) {
            signParams.putAll(params);
        }
        signParams.put("signature_method", SIGNATURE_METHOD);
        signParams.put("timestamp", timestamp);
        signParams.put("echostr", echostr);

        Map<String, String> sorted = new TreeMap<>(signParams);
        String query = buildQueryString(sorted);
        String signature = hmacSha256Hex(apiSecret, md5Hex(query).toUpperCase());
        String body = query + "&sign=" + signature;
        return new SignedPayload(body, timestamp, echostr);
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
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }

    private JsonNode requireSuccess(JsonNode response, String context) {
        if (response == null) {
            throw new ExchangeException("Unexpected response from LBank " + context + " API");
        }
        if (response.has("result")) {
            JsonNode resultNode = response.get("result");
            boolean ok = resultNode.isBoolean() ? resultNode.asBoolean() : "true".equalsIgnoreCase(resultNode.asText());
            if (!ok) {
                String code = textOf(response, "error_code", "code");
                String msg = textOf(response, "msg", "message");
                throw new ExchangeException("LBank " + context + " failed: " + code + " " + msg);
            }
        }
        if (response.has("error_code")) {
            String code = response.get("error_code").asText();
            if (!"0".equals(code) && !"00000".equals(code)) {
                String msg = textOf(response, "msg", "message");
                throw new ExchangeException("LBank " + context + " failed: " + code + " " + msg);
            }
        }
        if (response.has("data")) {
            return response.get("data");
        }
        return response;
    }

    private long getServerTime() {
        try {
            JsonNode response = publicGet("/v2/timestamp.do");
            JsonNode data = requireSuccess(response, "timestamp");
            JsonNode node = data.has("data") ? data.get("data") : data;
            if (node != null) {
                if (node.hasNonNull("timestamp")) {
                    return node.get("timestamp").asLong();
                }
                if (node.hasNonNull("serverTime")) {
                    return node.get("serverTime").asLong();
                }
                if (node.hasNonNull("data")) {
                    return node.get("data").asLong();
                }
                if (node.isNumber()) {
                    return node.asLong();
                }
            }
        } catch (Exception ignored) {
        }
        return System.currentTimeMillis();
    }

    private String randomEchoStr() {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        int len = 32;
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            int idx = ThreadLocalRandom.current().nextInt(chars.length());
            sb.append(chars.charAt(idx));
        }
        return sb.toString();
    }

    private String md5Hex(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] raw = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign LBank request", e);
        }
    }

    private String hmacSha256Hex(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign LBank request", e);
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

    private String normalizeNetworkParam(String network) {
        if (StringUtils.isBlank(network)) {
            return network;
        }
        int openIdx = network.indexOf('(');
        int closeIdx = network.indexOf(')');
        if (openIdx >= 0 && closeIdx > openIdx) {
            String inner = network.substring(openIdx + 1, closeIdx);
            if (StringUtils.isNotBlank(inner)) {
                return inner;
            }
        }
        return network;
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

    private record SignedPayload(String body, String timestamp, String echostr) {
    }

    private record SymbolInfo(String symbol, Integer quantityAccuracy, BigDecimal minTranQty) {
    }

    private record CoinConfig(String asset, List<NetworkConfig> networks) {
    }

    private record NetworkConfig(String name, String network, boolean depositEnabled, boolean withdrawEnabled, String withdrawFee,
                                 boolean isDefault, boolean memoRequired) {
    }
}
