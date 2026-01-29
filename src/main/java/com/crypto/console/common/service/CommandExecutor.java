package com.crypto.console.common.service;

import com.crypto.console.common.command.*;
import com.crypto.console.common.exchange.ExchangeClient;
import com.crypto.console.common.exchange.ExchangeRegistry;
import com.crypto.console.common.model.*;
import com.crypto.console.common.util.LogSanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;

public class CommandExecutor {
    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);

    private final ExchangeRegistry registry;
    private final MoveService moveService;
    private final FeeResolver feeResolver;

    public CommandExecutor(ExchangeRegistry registry, MoveService moveService, FeeResolver feeResolver) {
        this.registry = registry;
        this.moveService = moveService;
        this.feeResolver = feeResolver;
    }

    public CommandResult execute(Command command) {
        String raw = command.raw();
        log.info("COMMAND: {}", LogSanitizer.sanitize(raw));
        try {
            return switch (command.type()) {
                case HELP -> CommandResult.success(helpText());
                case INVALID -> CommandResult.failure(((InvalidCommand) command).error);
                case BALANCE -> handleBalance((BalanceCommand) command);
                case FEES -> handleFees((FeesCommand) command);
                case ORDERBOOK -> handleOrderBook((OrderBookCommand) command);
                case BUY -> handleBuy((BuyCommand) command);
                case SELL -> handleSell((SellCommand) command);
                case MOVE -> handleMove((MoveCommand) command);
                default -> CommandResult.failure("Unsupported command");
            };
        } catch (ExchangeException e) {
            log.warn("FAILED: {}", LogSanitizer.sanitize(e.getUserMessage()));
            return CommandResult.failure("FAILED: " + e.getUserMessage());
        } catch (Exception e) {
            log.error("FAILED: {}", LogSanitizer.sanitize(e.getMessage()));
            return CommandResult.failure("FAILED: " + e.getMessage());
        }
    }

    private CommandResult handleBalance(BalanceCommand cmd) {
        requireSecrets(cmd.exchange);
        ExchangeClient client = registry.getClient(cmd.exchange);
        Balance balance = client.getBalance(cmd.asset);
        String message = String.format("%s %s free=%s locked=%s",
                client.name(), cmd.asset, balance.free, balance.locked == null ? "0" : balance.locked);
        log.info("SUCCESS: {}", message);
        return CommandResult.success(message);
    }

    private CommandResult handleFees(FeesCommand cmd) {
        ExchangeClient client = registry.getClient(cmd.exchange);
        WithdrawalFees fees = feeResolver.resolveWithdrawalFees(client, cmd.exchange, cmd.asset);
        StringBuilder sb = new StringBuilder();
        sb.append(client.name()).append(" ").append(cmd.asset).append(" withdrawal fees:");
        fees.feeByNetwork.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .forEach(entry -> sb.append("\n  ").append(entry.getKey()).append(": ").append(entry.getValue()));
        log.info("SUCCESS: {}", LogSanitizer.sanitize(sb.toString()));
        return CommandResult.success(sb.toString());
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
        log.info("SUCCESS: {}", LogSanitizer.sanitize(sb.toString()));
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
        log.info("SUCCESS: {}", message);
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
        log.info("SUCCESS: {}", message);
        return CommandResult.success(message);
    }

    private CommandResult handleMove(MoveCommand cmd) {
        requireSecrets(cmd.from);
        requireSecrets(cmd.to);
        String message = moveService.move(cmd.from, cmd.to, cmd.amount, cmd.asset);
        log.info("SUCCESS: {}", LogSanitizer.sanitize(message));
        return CommandResult.success(message);
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
                "  sell <exchange> <baseAsset> <baseAmount> <quoteAsset>",
                "  balance <exchange> <asset>",
                "  fees <exchange> <asset>",
                "  orderbook <exchange> <base> <quote>",
                "  help",
                "  exit"
        );
    }
}


