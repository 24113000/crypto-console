package com.crypto.console.exchanges.gateio;

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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.*;

@Slf4j
public class GateIoClient extends BaseExchangeClient implements DepositNetworkProvider, DepositAddressProvider, DepositNetworkNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String API_PREFIX = "/api/v4";
    private static final String ALGORITHM = "HmacSHA512";
    private static final String EMPTY_BODY_SHA512 =
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e";

    public GateIoClient(AppProperties.ExchangeConfig cfg, SecretsProperties.ExchangeSecrets secrets) {
        super("gateio", cfg, secrets);
    }

    @Override
    public Balance getBalance(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for gateio");
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("currency", asset.toUpperCase());
        JsonNode response = signedGet("/spot/accounts", params, apiKey, apiSecret);
        if (response == null || !response.isArray()) {
            throw new ExchangeException("Unexpected response from Gate.io spot accounts API");
        }
        for (JsonNode entry : response) {
            String currency = entry.hasNonNull("currency") ? entry.get("currency").asText() : null;
            if (currency != null && currency.equalsIgnoreCase(asset)) {
                BigDecimal free = toDecimal(entry.get("available"));
                BigDecimal locked = toDecimal(entry.get("locked"));
                return new Balance(currency.toUpperCase(), free, locked);
            }
        }
        return new Balance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public WithdrawalFees getWithdrawalFees(String asset) {
        throw notImplemented("GET /wallet/withdraw_status (signed)");
    }

    @Override
    public OrderBook getOrderBook(String base, String quote, int depth) {
        throw notImplemented("GET /spot/order_book (public)");
    }

    @Override
    public BuyInfoResult buyInfo(String base, String quote, BigDecimal quoteAmount) {
        if (StringUtils.isBlank(base) || StringUtils.isBlank(quote)) {
            throw new ExchangeException("Base and quote assets are required");
        }
        if (quoteAmount == null || quoteAmount.signum() <= 0) {
            throw new ExchangeException("Quote amount must be positive");
        }

        String currencyPair = (base + "_" + quote).toUpperCase();
        CurrencyPairInfo info = getCurrencyPairInfo(currencyPair);
        if (info == null) {
            throw new ExchangeException("Invalid symbol: " + currencyPair);
        }
        if (!info.tradable) {
            throw new ExchangeException("Gate.io symbol not tradable: " + currencyPair);
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("currency_pair", currencyPair);
        params.put("limit", "200");
        JsonNode response = publicGet("/spot/order_book", params);
        if (response == null || response.get("asks") == null || !response.get("asks").isArray()) {
            throw new ExchangeException("Unexpected response from Gate.io order book API");
        }

        BigDecimal remainingQuote = quoteAmount;
        BigDecimal spentQuote = BigDecimal.ZERO;
        BigDecimal boughtBase = BigDecimal.ZERO;
        List<BuyInfoItem> affectedItems = new ArrayList<>();

        for (JsonNode ask : response.get("asks")) {
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
            throw new ExchangeException("No ask liquidity available for " + currencyPair);
        }

        BigDecimal averagePrice = spentQuote.divide(boughtBase, 18, RoundingMode.HALF_UP);
        return new BuyInfoResult(currencyPair, quoteAmount, spentQuote, boughtBase, averagePrice, List.copyOf(affectedItems));
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
            throw new ExchangeException("Missing API credentials for gateio");
        }

        List<ChainInfo> chains = getCurrencyChains(asset);
        String chain = resolveChain(chains, network, true, asset);
        if (StringUtils.isBlank(chain)) {
            throw new ExchangeException("Gate.io does not support withdrawals for asset: " + asset.toUpperCase());
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("currency", asset.toUpperCase());
        body.put("address", address);
        body.put("amount", amount.toPlainString());
        body.put("chain", chain);
        if (StringUtils.isNotBlank(memoOrNull)) {
            body.put("memo", memoOrNull);
        }

        JsonNode response = signedPost("/withdrawals", body, apiKey, apiSecret);
        if (response == null) {
            throw new ExchangeException("Unexpected response from Gate.io withdraw API");
        }
        String id = response.hasNonNull("id") ? response.get("id").asText() : null;
        if (StringUtils.isBlank(id)) {
            throw new ExchangeException("Missing withdrawal id from Gate.io withdraw API");
        }
        String status = response.hasNonNull("status") ? response.get("status").asText() : "SUBMITTED";
        return new WithdrawResult(id, status, "withdraw submitted");
    }

    @Override
    public ExchangeTime syncTime() {
        JsonNode response = publicGet("/spot/time", null);
        if (response == null || response.get("server_time") == null) {
            throw new ExchangeException("Unexpected response from Gate.io time API");
        }
        long serverTime = response.get("server_time").asLong();
        long offset = serverTime - System.currentTimeMillis();
        return new ExchangeTime(serverTime, offset);
    }

    @Override
    public Set<String> getDepositNetworks(String asset) {
        if (StringUtils.isBlank(asset)) {
            throw new ExchangeException("Asset is required");
        }
        List<ChainInfo> chains = getCurrencyChains(asset);
        Set<String> networks = new HashSet<>();
        for (ChainInfo chain : chains) {
            if (chain.depositEnabled && StringUtils.isNotBlank(chain.chain)) {
                networks.add(chain.chain.trim().toUpperCase());
            }
        }
        if (networks.isEmpty()) {
            throw new ExchangeException("Gate.io does not support deposits for asset: " + asset.toUpperCase());
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
            throw new ExchangeException("Missing API credentials for gateio");
        }

        List<ChainInfo> chains = getCurrencyChains(asset);
        String chain = resolveChain(chains, network, false, asset);
        if (StringUtils.isBlank(chain)) {
            throw new ExchangeException("Gate.io does not support deposits for asset: " + asset.toUpperCase());
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("currency", asset.toUpperCase());
        JsonNode response = signedGet("/wallet/deposit_address", params, apiKey, apiSecret);
        if (response == null) {
            throw new ExchangeException("Unexpected response from Gate.io deposit address API");
        }
        JsonNode list = response.get("multichain_addresses");
        String normalizedTarget = normalizeNetworkForMatch(chain, asset);
        if (list != null && list.isArray()) {
            for (JsonNode item : list) {
                String chainName = item.hasNonNull("chain") ? item.get("chain").asText() : null;
                String address = item.hasNonNull("address") ? item.get("address").asText() : null;
                int obtainFailed = item.hasNonNull("obtain_failed") ? item.get("obtain_failed").asInt(0) : 0;
                if (StringUtils.isBlank(address) || obtainFailed != 0) {
                    continue;
                }
                if (StringUtils.isBlank(normalizedTarget)) {
                    return address;
                }
                String normalizedChain = normalizeNetworkForMatch(chainName, asset);
                if (normalizedTarget.equalsIgnoreCase(normalizedChain)) {
                    return address;
                }
            }
            if (StringUtils.isNotBlank(normalizedTarget)) {
                throw new ExchangeException("Gate.io deposit address not found for network " + chain);
            }
        }
        JsonNode addressNode = response.get("address");
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
            case "TRON", "TRC20", "TRX" -> "TRX";
            case "OPTIMISM", "OP" -> "OPTIMISM";
            case "HECO", "HRC20" -> "HRC20";
            case "KAIA" -> "KAIA";
            case "CELO" -> "CELO";
            default -> cleaned;
        };
    }

    private OrderResult submitMarketOrder(OrderSide side, String base, String quote, BigDecimal amount, boolean isQuoteAmount) {
        String apiKey = getApiKey();
        String apiSecret = getApiSecret();
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(apiSecret)) {
            throw new ExchangeException("Missing API credentials for gateio");
        }

        String currencyPair = (base + "_" + quote).toUpperCase();
        CurrencyPairInfo info = getCurrencyPairInfo(currencyPair);
        if (info == null) {
            throw new ExchangeException("Invalid symbol: " + currencyPair);
        }
        if (!info.tradable) {
            throw new ExchangeException("Gate.io symbol not tradable: " + currencyPair);
        }

        if (isQuoteAmount) {
            if (info.minQuoteAmount != null && info.minQuoteAmount.signum() > 0
                    && amount.compareTo(info.minQuoteAmount) < 0) {
                throw new ExchangeException("Order value " + amount + " below min notional " + info.minQuoteAmount + " for " + currencyPair);
            }
        } else {
            BigDecimal qty = applyAmountPrecision(info, amount);
            if (qty.signum() <= 0) {
                throw new ExchangeException("Sell quantity below minimum lot size for " + currencyPair);
            }
            if (info.minBaseAmount != null && info.minBaseAmount.signum() > 0
                    && qty.compareTo(info.minBaseAmount) < 0) {
                throw new ExchangeException("Sell quantity " + qty + " below min lot size " + info.minBaseAmount + " for " + currencyPair);
            }
            if (info.minQuoteAmount != null && info.minQuoteAmount.signum() > 0) {
                BigDecimal price = getLastPrice(currencyPair);
                if (price != null && price.signum() > 0) {
                    BigDecimal notional = qty.multiply(price);
                    if (notional.compareTo(info.minQuoteAmount) < 0) {
                        throw new ExchangeException("Order value " + notional + " below min notional " + info.minQuoteAmount + " for " + currencyPair);
                    }
                }
            }
            amount = qty;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("currency_pair", currencyPair);
        body.put("type", "market");
        body.put("account", "spot");
        body.put("side", side == OrderSide.BUY ? "buy" : "sell");
        body.put("amount", amount.toPlainString());
        body.put("time_in_force", "ioc");

        JsonNode response = signedPost("/spot/orders", body, apiKey, apiSecret);
        if (response == null) {
            throw new ExchangeException("Unexpected response from Gate.io order API");
        }
        String orderId = response.hasNonNull("id") ? response.get("id").asText() : null;
        if (StringUtils.isBlank(orderId)) {
            throw new ExchangeException("Missing order id from Gate.io order API");
        }
        String status = response.hasNonNull("status") ? response.get("status").asText() : "SUBMITTED";
        return new OrderResult(orderId, status, "market order submitted");
    }

    private CurrencyPairInfo getCurrencyPairInfo(String currencyPair) {
        JsonNode response = publicGet("/spot/currency_pairs/" + currencyPair, null);
        if (response == null || response.isNull()) {
            return null;
        }
        BigDecimal minBase = toDecimal(response.get("min_base_amount"));
        BigDecimal minQuote = toDecimal(response.get("min_quote_amount"));
        Integer amountPrecision = response.hasNonNull("amount_precision") ? response.get("amount_precision").asInt() : null;
        String tradeStatus = response.hasNonNull("trade_status") ? response.get("trade_status").asText() : null;
        boolean tradable = "tradable".equalsIgnoreCase(tradeStatus);
        return new CurrencyPairInfo(minBase, minQuote, amountPrecision, tradable);
    }

    private BigDecimal getLastPrice(String currencyPair) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("currency_pair", currencyPair);
        JsonNode response = publicGet("/spot/tickers", params);
        if (response != null && response.isArray() && response.size() > 0) {
            JsonNode item = response.get(0);
            return toDecimal(item.get("last"));
        }
        return null;
    }

    private BigDecimal applyAmountPrecision(CurrencyPairInfo info, BigDecimal quantity) {
        if (info == null || quantity == null) {
            return quantity;
        }
        Integer precision = info.amountPrecision;
        if (precision != null && precision >= 0) {
            quantity = quantity.setScale(precision, java.math.RoundingMode.DOWN);
        }
        return quantity.stripTrailingZeros();
    }

    private List<ChainInfo> getCurrencyChains(String asset) {
        String assetUpper = asset.toUpperCase();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("currency", assetUpper);
        JsonNode response = publicGet("/wallet/currency_chains", params);
        if (response == null || !response.isArray()) {
            throw new ExchangeException("Gate.io does not support asset: " + assetUpper);
        }
        List<ChainInfo> chains = new ArrayList<>();
        for (JsonNode item : response) {
            String chain = item.hasNonNull("chain") ? item.get("chain").asText() : null;
            int disabled = item.hasNonNull("is_disabled") ? item.get("is_disabled").asInt() : 0;
            int depositDisabled = item.hasNonNull("is_deposit_disabled") ? item.get("is_deposit_disabled").asInt() : 0;
            int withdrawDisabled = item.hasNonNull("is_withdraw_disabled") ? item.get("is_withdraw_disabled").asInt() : 0;
            boolean depositEnabled = disabled == 0 && depositDisabled == 0;
            boolean withdrawEnabled = disabled == 0 && withdrawDisabled == 0;
            chains.add(new ChainInfo(chain, depositEnabled, withdrawEnabled));
        }
        if (chains.isEmpty()) {
            throw new ExchangeException("Gate.io does not support asset: " + assetUpper);
        }
        return chains;
    }

    private String resolveChain(List<ChainInfo> chains, String network, boolean forWithdraw, String asset) {
        if (chains == null || chains.isEmpty()) {
            return null;
        }
        List<ChainInfo> candidates = new ArrayList<>();
        for (ChainInfo chain : chains) {
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
        String normalized = normalizeNetworkForMatch(network, asset);
        if (StringUtils.isBlank(normalized)) {
            if (candidates.size() == 1) {
                return normalizeChainParam(candidates.get(0).chain);
            }
            StringBuilder available = new StringBuilder();
            for (ChainInfo chain : candidates) {
                if (StringUtils.isBlank(chain.chain)) {
                    continue;
                }
                if (!available.isEmpty()) {
                    available.append(", ");
                }
                available.append(chain.chain);
            }
            throw new ExchangeException("Network is required for " + asset.toUpperCase() + " (available: " + available + ")");
        }
        for (ChainInfo chain : candidates) {
            if (StringUtils.isBlank(chain.chain)) {
                continue;
            }
            String normalizedChain = normalizeNetworkForMatch(chain.chain, asset);
            if (normalized.equalsIgnoreCase(normalizedChain)) {
                return normalizeChainParam(chain.chain);
            }
        }
        throw new ExchangeException("Gate.io does not support network " + network + " for asset " + asset.toUpperCase());
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

    private String normalizeNetworkForMatch(String network, String asset) {
        String normalized = normalizeDepositNetwork(network);
        if (StringUtils.isBlank(normalized) || StringUtils.isBlank(asset)) {
            return normalized;
        }
        String assetUpper = asset.toUpperCase();
        if (normalized.startsWith(assetUpper) && normalized.length() > assetUpper.length()) {
            normalized = normalized.substring(assetUpper.length());
        } else if (normalized.endsWith(assetUpper) && normalized.length() > assetUpper.length()) {
            normalized = normalized.substring(0, normalized.length() - assetUpper.length());
        }
        return normalizeDepositNetwork(normalized);
    }

    private JsonNode signedGet(String path, Map<String, String> params, String apiKey, String apiSecret) {
        String query = params == null ? "" : buildQueryString(params);
        String uri = query.isEmpty() ? API_PREFIX + path : API_PREFIX + path + "?" + query;
        String requestPath = API_PREFIX + path;
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000L);
        String signature = sign("GET", requestPath, query, "", timestamp, apiSecret);
        LOG.info("gateio GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header("KEY", apiKey)
                .header("Timestamp", timestamp)
                .header("SIGN", signature)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "Gate.io request failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode signedPost(String path, Map<String, Object> body, String apiKey, String apiSecret) {
        String jsonBody = "";
        try {
            if (body != null) {
                jsonBody = MAPPER.writeValueAsString(body);
            }
        } catch (Exception e) {
            throw new ExchangeException("Failed to serialize Gate.io request body", e);
        }
        String uri = API_PREFIX + path;
        String requestPath = API_PREFIX + path;
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000L);
        String signature = sign("POST", requestPath, "", jsonBody, timestamp, apiSecret);
        LOG.info("gateio POST {}", LogSanitizer.sanitize(uri));
        return webClient.post()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header("KEY", apiKey)
                .header("Timestamp", timestamp)
                .header("SIGN", signature)
                .bodyValue(jsonBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String bodyText = ex.getResponseBodyAsString();
                    String msg = "Gate.io request failed: HTTP " + ex.getStatusCode().value();
                    if (bodyText != null && !bodyText.isBlank()) {
                        msg = msg + " body=" + bodyText;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private JsonNode publicGet(String path, Map<String, String> params) {
        String query = params == null ? "" : buildQueryString(params);
        String uri = query.isEmpty() ? API_PREFIX + path : API_PREFIX + path + "?" + query;
        LOG.info("gateio GET {}", LogSanitizer.sanitize(uri));
        return webClient.get()
                .uri(uri)
                .header(HttpHeaders.USER_AGENT, "crypto-console")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(org.springframework.web.reactive.function.client.WebClientResponseException.class, ex -> {
                    String body = ex.getResponseBodyAsString();
                    String msg = "Gate.io request failed: HTTP " + ex.getStatusCode().value();
                    if (body != null && !body.isBlank()) {
                        msg = msg + " body=" + body;
                    }
                    return reactor.core.publisher.Mono.error(new ExchangeException(msg, ex));
                })
                .block();
    }

    private String sign(String method, String requestPath, String query, String body, String timestamp, String secret) {
        String payloadHash = sha512Hex(body == null ? "" : body);
        String signatureString = method.toUpperCase() + "\n"
                + requestPath + "\n"
                + (query == null ? "" : query) + "\n"
                + payloadHash + "\n"
                + timestamp;
        return hmacSha512Hex(secret, signatureString);
    }

    private String sha512Hex(String payload) {
        if (payload == null || payload.isEmpty()) {
            return EMPTY_BODY_SHA512;
        }
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-512");
            byte[] digest = md.digest(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to hash Gate.io request body", e);
        }
    }

    private String hmacSha512Hex(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new ExchangeException("Failed to sign Gate.io request", e);
        }
    }

    private String buildQueryString(Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return "";
        }
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

    private record CurrencyPairInfo(BigDecimal minBaseAmount, BigDecimal minQuoteAmount, Integer amountPrecision,
                                    boolean tradable) {
    }

    private record ChainInfo(String chain, boolean depositEnabled, boolean withdrawEnabled) {
    }
}

