package com.crypto.console.common.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.Map;

@ConfigurationProperties(prefix = "secrets")
@Validated
public class SecretsProperties {
    @NotEmpty
    private Map<String, @Valid ExchangeSecrets> exchanges;

    public static class ExchangeSecrets {
        @NotBlank
        private String apiKey;
        @NotBlank
        private String apiSecret;
        private String apiPassphrase;
        private String apiMemo;
        private String apiVersion;

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }

        public String getApiSecret() {
            return apiSecret;
        }

        public void setApiSecret(String apiSecret) {
            this.apiSecret = apiSecret;
        }

        public String getApiPassphrase() {
            return apiPassphrase;
        }

        public void setApiPassphrase(String apiPassphrase) {
            this.apiPassphrase = apiPassphrase;
        }

        public String getApiMemo() {
            return apiMemo;
        }

        public void setApiMemo(String apiMemo) {
            this.apiMemo = apiMemo;
        }

        public String getApiVersion() {
            return apiVersion;
        }

        public void setApiVersion(String apiVersion) {
            this.apiVersion = apiVersion;
        }
    }

    public Map<String, ExchangeSecrets> getExchanges() {
        return exchanges;
    }

    public void setExchanges(Map<String, ExchangeSecrets> exchanges) {
        this.exchanges = exchanges;
    }
}

