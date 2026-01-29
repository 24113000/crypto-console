package com.crypto.console.common.exchange.impl;

import com.crypto.console.common.model.ExchangeException;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Supplier;

public class ExchangeHttpClient {
    private final int maxAttempts;

    public ExchangeHttpClient(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public <T> T executeWithRetry(Supplier<Mono<T>> call) {
        int attempt = 0;
        long backoffMillis = 1000;
        while (true) {
            attempt++;
            try {
                return call.get().block(Duration.ofSeconds(30));
            } catch (Exception ex) {
                if (attempt >= maxAttempts || !isRetryable(ex)) {
                    throw new ExchangeException("HTTP request failed: " + ex.getMessage(), ex);
                }
                try {
                    Thread.sleep(backoffMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ExchangeException("Retry interrupted", ie);
                }
                backoffMillis *= 2;
            }
        }
    }

    private boolean isRetryable(Exception ex) {
        if (ex instanceof WebClientResponseException wcre) {
            HttpStatusCode status = wcre.getStatusCode();
            int code = status.value();
            return code == 429 || code >= 500;
        }
        return ex.getMessage() != null && ex.getMessage().toLowerCase().contains("timeout");
    }
}



