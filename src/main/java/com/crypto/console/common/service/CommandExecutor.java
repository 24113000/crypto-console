package com.crypto.console.common.service;

import com.crypto.console.common.command.Command;
import com.crypto.console.common.command.impl.AddressCommand;
import com.crypto.console.common.command.impl.BalanceCommand;
import com.crypto.console.common.command.impl.BalancesCommand;
import com.crypto.console.common.command.impl.BuyCommand;
import com.crypto.console.common.command.impl.BuyInfoCommand;
import com.crypto.console.common.command.impl.SellInfoCommand;
import com.crypto.console.common.command.impl.DepositCommand;
import com.crypto.console.common.command.impl.SpreadCommand;
import com.crypto.console.common.command.impl.InvalidCommand;
import com.crypto.console.common.command.impl.MoveCommand;
import com.crypto.console.common.command.impl.OrderBookCommand;
import com.crypto.console.common.command.impl.SellCommand;
import com.crypto.console.common.exchange.DepositAddressProvider;
import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.exchange.impl.ExchangeRegistry;
import com.crypto.console.common.model.Balance;
import com.crypto.console.common.model.CommandResult;
import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.model.BuyInfoResult;
import com.crypto.console.common.model.OrderBook;
import com.crypto.console.common.model.OrderBookEntry;
import com.crypto.console.common.model.OrderResult;
import com.crypto.console.common.util.LogSanitizer;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class CommandExecutor {

    private final ExchangeRegistry registry;
    private final MoveService moveService;
    private final DepositNetworkResolver networkResolver;

    public CommandExecutor(ExchangeRegistry registry, MoveService moveService, DepositNetworkResolver networkResolver) {
        this.registry = registry;
        this.moveService = moveService;
        this.networkResolver = networkResolver;
    }

    public CommandResult execute(Command command) {
        String raw = command.raw();
        LOG.info("COMMAND: {}", LogSanitizer.sanitize(raw));
        try {
            return switch (command.type()) {
                case HELP -> CommandResult.success(helpText());
                case INVALID -> CommandResult.failure(((InvalidCommand) command).error);
                case BALANCE -> handleBalance((BalanceCommand) command);
                case BALANCES -> handleBalances((BalancesCommand) command);
                case ORDERBOOK -> handleOrderBook((OrderBookCommand) command);
                case BUY -> handleBuy((BuyCommand) command);
                case BUYINFO -> handleBuyInfo((BuyInfoCommand) command);
                case SELLINFO -> handleSellInfo((SellInfoCommand) command);
                case SPREAD -> handleSpread((SpreadCommand) command);
                case SELL -> handleSell((SellCommand) command);
                case MOVE -> handleMove((MoveCommand) command);
                case DEPOSIT -> handleDeposit((DepositCommand) command);
                case ADDRESS -> handleAddress((AddressCommand) command);
                default -> CommandResult.failure("Unsupported command");
            };
        } catch (ExchangeException e) {
            logError(e.getUserMessage());
            return CommandResult.failure("FAILED: " + e.getUserMessage());
        } catch (Exception e) {
            logError(e.getMessage());
            return CommandResult.failure("FAILED: " + e.getMessage());
        }
    }

    private static void logError(String e) {
        LOG.error("FAILED: {}", LogSanitizer.sanitize(e));
    }

    private CommandResult handleBalance(BalanceCommand cmd) {
        requireSecrets(cmd.exchange);
        ExchangeClient client = registry.getClient(cmd.exchange);
        Balance balance = client.getBalance(cmd.asset);
        String message = String.format("%s %s free=%s locked=%s",
                client.name(), cmd.asset, balance.free, balance.locked == null ? "0" : balance.locked);
        logSuccess(message);
        return CommandResult.success(message);
    }

    private CommandResult handleBalances(BalancesCommand cmd) {
        List<BalanceRow> rows = new ArrayList<>();
        BigDecimal total = BigDecimal.ZERO;
        for (String exchange : registry.getAvailableExchanges()) {
            try {
                requireSecrets(exchange);
                ExchangeClient client = registry.getClient(exchange);
                Balance balance = client.getBalance(cmd.asset);
                String totalBalance = formatTotalBalance(balance);
                rows.add(new BalanceRow(client.name(), totalBalance, "ok"));
                total = total.add(parseBalanceValue(totalBalance));
            } catch (Exception e) {
                rows.add(new BalanceRow(exchange, "", "error"));
            }
        }

        String message = formatBalancesTable(rows, cmd.asset, total);
        logSuccess(LogSanitizer.sanitize(message));
        return CommandResult.success(message);
    }

    private CommandResult handleOrderBook(OrderBookCommand cmd) {
        ExchangeClient client = registry.getClient(cmd.exchange);
        if (!client.capabilities().supportsOrderBook) {
            throw new ExchangeException("Order book not supported for " + cmd.exchange);
        }
        OrderBook book = client.getOrderBook(cmd.base, cmd.quote, 10);
        StringBuilder sb = new StringBuilder();
        sb.append(client.name()).append(" ").append(book.symbol).append(" order book (top 10)");
        sb.append("\nBids:");
        for (OrderBookEntry entry : book.bids) {
            sb.append("\n  ").append(entry.price).append(" x ").append(entry.quantity);
        }
        sb.append("\nAsks:");
        for (OrderBookEntry entry : book.asks) {
            sb.append("\n  ").append(entry.price).append(" x ").append(entry.quantity);
        }
        logSuccess(LogSanitizer.sanitize(sb.toString()));
        return CommandResult.success(sb.toString());
    }

    private CommandResult handleBuy(BuyCommand cmd) {
        requireSecrets(cmd.exchange);
        ExchangeClient client = registry.getClient(cmd.exchange);
        if (!client.capabilities().supportsMarketOrders) {
            throw new ExchangeException("Market orders not supported for " + cmd.exchange);
        }
        OrderResult result = client.marketBuy(cmd.baseAsset, cmd.quoteAsset, cmd.quoteAmount);
        String message = "BUY placed on " + client.name() + ": " + result.status + " id=" + result.orderId;
        logSuccess(message);
        return CommandResult.success(message);
    }

    private CommandResult handleBuyInfo(BuyInfoCommand cmd) {
        ExchangeClient client = registry.getClient(cmd.exchange);
        BuyInfoResult result = client.buyInfo(cmd.baseAsset, cmd.quoteAsset, cmd.quoteAmount);
        String message = "BUYINFO " + client.name()
                + " " + result.symbol
                + ": for " + result.requestedQuoteAmount + " " + cmd.quoteAsset
                + " -> buy " + result.boughtBaseAmount + " " + cmd.baseAsset
                + ", avg price " + result.averagePrice + " " + cmd.quoteAsset;
        if (result.affectedOrderBookItems != null && !result.affectedOrderBookItems.isEmpty()) {
            StringBuilder sb = new StringBuilder(message);
            sb.append("\nAffected asks:");
            for (var item : result.affectedOrderBookItems) {
                sb.append("\n")
                        .append("price ")
                        .append(item.price)
                        .append(": ")
                        .append(item.filledBaseAmount)
                        .append(" ")
                        .append(cmd.baseAsset)
                        .append(" - ")
                        .append(item.spentQuoteAmount)
                        .append(" ")
                        .append(cmd.quoteAsset)
                        .append(" (level cost)");
            }
            message = sb.toString();
        }
        logSuccess(message);
        return CommandResult.success(message);
    }

    private CommandResult handleSell(SellCommand cmd) {
        requireSecrets(cmd.exchange);
        ExchangeClient client = registry.getClient(cmd.exchange);
        if (!client.capabilities().supportsMarketOrders) {
            throw new ExchangeException("Market orders not supported for " + cmd.exchange);
        }
        OrderResult result = client.marketSell(cmd.baseAsset, cmd.quoteAsset, cmd.baseAmount);
        String message = "SELL placed on " + client.name() + ": " + result.status + " id=" + result.orderId;
        logSuccess(message);
        return CommandResult.success(message);
    }

    private CommandResult handleSellInfo(SellInfoCommand cmd) {
        ExchangeClient client = registry.getClient(cmd.exchange);
        BuyInfoResult result = client.sellInfo(cmd.baseAsset, cmd.quoteAsset, cmd.quoteAmount);
        String message = "SELLINFO " + client.name()
                + " " + result.symbol
                + ": for " + result.requestedQuoteAmount + " " + cmd.quoteAsset
                + " -> sell " + result.boughtBaseAmount + " " + cmd.baseAsset
                + ", avg price " + result.averagePrice + " " + cmd.quoteAsset;
        if (result.affectedOrderBookItems != null && !result.affectedOrderBookItems.isEmpty()) {
            StringBuilder sb = new StringBuilder(message);
            sb.append("\nAffected bids:");
            for (var item : result.affectedOrderBookItems) {
                sb.append("\n")
                        .append("price ")
                        .append(item.price)
                        .append(": ")
                        .append(item.filledBaseAmount)
                        .append(" ")
                        .append(cmd.baseAsset)
                        .append(" - ")
                        .append(item.spentQuoteAmount)
                        .append(" ")
                        .append(cmd.quoteAsset)
                        .append(" (level proceeds)");
            }
            message = sb.toString();
        }
        logSuccess(message);
        return CommandResult.success(message);
    }

    private CommandResult handleSpread(SpreadCommand cmd) {
        ExchangeClient ex1 = registry.getClient(cmd.exchange1);
        ExchangeClient ex2 = registry.getClient(cmd.exchange2);
        BuyInfoResult buyInfo = ex1.buyInfo(cmd.baseAsset, cmd.quoteAsset, cmd.quoteAmount);
        BuyInfoResult sellInfo = ex2.sellInfo(cmd.baseAsset, cmd.quoteAsset, cmd.quoteAmount);
        String withdrawStatus = ex1.getWithdrawStatus(cmd.baseAsset);
        java.math.BigDecimal ask = buyInfo.averagePrice;
        java.math.BigDecimal bid = sellInfo.averagePrice;
        if (ask == null || ask.signum() <= 0 || bid == null || bid.signum() <= 0) {
            throw new ExchangeException("Invalid prices returned for spread calculation");
        }
        java.math.BigDecimal spread = bid.subtract(ask)
                .divide(ask, 6, java.math.RoundingMode.HALF_UP)
                .multiply(new java.math.BigDecimal("100"));
        String spreadPct = spread.setScale(2, java.math.RoundingMode.HALF_UP).toPlainString();
        String message = "SPREAD " + ex1.name() + "->" + ex2.name()
                + " " + cmd.baseAsset + "/" + cmd.quoteAsset
                + " amount=" + cmd.quoteAmount + " " + cmd.quoteAsset
                + ": ask=" + ask + " (" + ex1.name() + "), bid=" + bid + " (" + ex2.name() + ")"
                + ", spread=" + spreadPct + "%"
                + System.lineSeparator()
                + withdrawStatus;
        logSuccess(message);
        return CommandResult.success(message);
    }

    private CommandResult handleMove(MoveCommand cmd) {
        requireSecrets(cmd.from);
        requireSecrets(cmd.to);
        String message = moveService.move(cmd.from, cmd.to, cmd.amount, cmd.asset);
        logSuccess(LogSanitizer.sanitize(message));
        return CommandResult.success(message);
    }

    private CommandResult handleDeposit(DepositCommand cmd) {
        requireSecrets(cmd.exchange);
        ExchangeClient client = registry.getClient(cmd.exchange);
        var networks = networkResolver.resolveDepositNetworks(client, cmd.exchange, cmd.asset);
        StringBuilder sb = new StringBuilder();
        sb.append("supported networks:");
        networks.stream().sorted().forEach(n -> sb.append("\n").append(n));
        String message = sb.toString();
        logSuccess(LogSanitizer.sanitize(message));
        return CommandResult.success(message);
    }

    private CommandResult handleAddress(AddressCommand cmd) {
        requireSecrets(cmd.exchange);
        ExchangeClient client = registry.getClient(cmd.exchange);
        if (!(client instanceof DepositAddressProvider provider)) {
            throw new ExchangeException("Deposit address not supported for " + cmd.exchange);
        }
        String address = provider.getDepositAddress(cmd.asset, cmd.network);
        if (address == null || address.isBlank()) {
            throw new ExchangeException("No deposit address returned for " + cmd.exchange + " " + cmd.asset + " " + cmd.network);
        }
        logSuccess(LogSanitizer.sanitize(address));
        return CommandResult.success(address);
    }

    private static void logSuccess(String message) {
        LOG.info("SUCCESS: {}", message);
    }

    private String formatBalancesTable(List<BalanceRow> rows, String asset, BigDecimal total) {
        int exchangeWidth = "exchange".length();
        int balanceWidth = (asset + " balance").length();
        int statusWidth = "success".length();

        for (BalanceRow row : rows) {
            exchangeWidth = Math.max(exchangeWidth, row.exchange.length());
            balanceWidth = Math.max(balanceWidth, row.balance.length());
            statusWidth = Math.max(statusWidth, row.status.length());
        }

        String format = "%-" + exchangeWidth + "s | %-" + balanceWidth + "s | %-" + statusWidth + "s";
        String separator = "-".repeat(exchangeWidth) + "-+-"
                + "-".repeat(balanceWidth) + "-+-"
                + "-".repeat(statusWidth);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(format, "exchange", asset + " balance", "success"));
        sb.append("\n").append(separator);
        for (BalanceRow row : rows) {
            sb.append("\n").append(String.format(format, row.exchange, row.balance, row.status));
        }
        sb.append("\n").append(separator);
        sb.append("\n").append(String.format(format, "total", total.stripTrailingZeros().toPlainString(), "ok"));
        return sb.toString();
    }

    private String formatTotalBalance(Balance balance) {
        BigDecimal free = balance.free == null ? BigDecimal.ZERO : balance.free;
        BigDecimal locked = balance.locked == null ? BigDecimal.ZERO : balance.locked;
        return free.add(locked).stripTrailingZeros().toPlainString();
    }

    private BigDecimal parseBalanceValue(String value) {
        if (value == null || value.isBlank()) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(value);
    }

    private void requireSecrets(String exchange) {
        if (!registry.hasSecrets(exchange)) {
            throw new ExchangeException("Missing API credentials for exchange: " + exchange);
        }
    }

    private String helpText() {
        return String.join("\n",
                "Commands:",
                "  move <from> <to> <amount> <asset>",
                "  buy <exchange> <baseAsset> <quoteAmount> <quoteAsset>",
                "  buyinfo <exchange> <baseAsset> <quoteAmount> <quoteAsset>",
                "  sellinfo <exchange> <baseAsset> <quoteAmount> <quoteAsset>",
                "  spread <ex1> <ex2> <baseAsset> <quoteAmount> <quoteAsset>",
                "  sell <exchange> <baseAsset> <baseAmount> <quoteAsset>",
                "  balance <exchange> <asset>",
                "  balances <asset>",
                "  deposit <exchange> <asset>",
                "  address <exchange> <asset> <network>",
                "  orderbook <exchange> <base> <quote>",
                "  help",
                "  exit"
        );
    }

    private record BalanceRow(String exchange, String balance, String status) {
    }
}
