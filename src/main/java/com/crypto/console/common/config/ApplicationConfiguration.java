package com.crypto.console.common.config;

import com.crypto.console.common.command.impl.CommandParser;
import com.crypto.console.common.exchange.impl.ExchangeRegistry;
import com.crypto.console.common.properties.AppProperties;
import com.crypto.console.common.properties.SecretsProperties;
import com.crypto.console.common.service.CommandExecutor;
import com.crypto.console.common.service.DepositNetworkResolver;
import com.crypto.console.common.service.FeeResolver;
import com.crypto.console.common.service.MoveService;
import com.crypto.console.common.service.NetworkSelector;
import com.crypto.console.repl.ReplRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({AppProperties.class, SecretsProperties.class})
public class ApplicationConfiguration {
    @Bean
    public ExchangeRegistry exchangeRegistry(AppProperties appProperties, SecretsProperties secretsProperties) {
        return ExchangeRegistry.create(appProperties, secretsProperties);
    }

    @Bean
    public FeeResolver feeResolver(AppProperties appProperties) {
        return new FeeResolver(appProperties);
    }

    @Bean
    public DepositNetworkResolver depositNetworkResolver(AppProperties appProperties) {
        return new DepositNetworkResolver(appProperties);
    }

    @Bean
    public NetworkSelector networkSelector() {
        return new NetworkSelector();
    }

    @Bean
    public MoveService moveService(ExchangeRegistry registry, AppProperties appProperties, FeeResolver feeResolver, DepositNetworkResolver depositNetworkResolver, NetworkSelector networkSelector) {
        return new MoveService(registry, appProperties, feeResolver, depositNetworkResolver, networkSelector);
    }

    @Bean
    public CommandExecutor commandExecutor(ExchangeRegistry registry, MoveService moveService, FeeResolver feeResolver) {
        return new CommandExecutor(registry, moveService, feeResolver);
    }

    @Bean
    public CommandParser commandParser() {
        return new CommandParser();
    }

    @Bean
    public ReplRunner replRunner(CommandParser parser, CommandExecutor executor) {
        return new ReplRunner(parser, executor);
    }
}

