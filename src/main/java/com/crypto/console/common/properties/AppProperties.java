package com.crypto.console.common.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "app")
@Validated
public class AppProperties {
    @Valid
    private PollingConfig polling = new PollingConfig();
    private Map<String, List<String>> networkPriority;
    @NotEmpty
    private Map<String, @Valid ExchangeConfig> exchanges;
    private Map<String, Map<String, Map<String, AddressConfig>>> withdrawalAddresses;
    private Map<String, Map<String, List<String>>> supportedNetworksFallback;
    private Map<String, Map<String, Map<String, String>>> withdrawFeesFallback;

    public static class PollingConfig {
        @Min(1)
        private int intervalSeconds = 10;
        @Min(1)
        private int maxWaitSeconds = 1800;

        public int getIntervalSeconds() {
            return intervalSeconds;
        }

        public void setIntervalSeconds(int intervalSeconds) {
            this.intervalSeconds = intervalSeconds;
        }

        public int getMaxWaitSeconds() {
            return maxWaitSeconds;
        }

        public void setMaxWaitSeconds(int maxWaitSeconds) {
            this.maxWaitSeconds = maxWaitSeconds;
        }
    }

    public static class ExchangeConfig {
        @NotBlank
        private String baseUrl;

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }
    }

    public static class AddressConfig {
        private String address;
        private String memo;
        private Boolean memoRequired;

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getMemo() {
            return memo;
        }

        public void setMemo(String memo) {
            this.memo = memo;
        }

        public Boolean getMemoRequired() {
            return memoRequired;
        }

        public void setMemoRequired(Boolean memoRequired) {
            this.memoRequired = memoRequired;
        }
    }

    public PollingConfig getPolling() {
        return polling;
    }

    public void setPolling(PollingConfig polling) {
        this.polling = polling;
    }

    public Map<String, List<String>> getNetworkPriority() {
        return networkPriority;
    }

    public void setNetworkPriority(Map<String, List<String>> networkPriority) {
        this.networkPriority = networkPriority;
    }

    public Map<String, ExchangeConfig> getExchanges() {
        return exchanges;
    }

    public void setExchanges(Map<String, ExchangeConfig> exchanges) {
        this.exchanges = exchanges;
    }

    public Map<String, Map<String, Map<String, AddressConfig>>> getWithdrawalAddresses() {
        return withdrawalAddresses;
    }

    public void setWithdrawalAddresses(Map<String, Map<String, Map<String, AddressConfig>>> withdrawalAddresses) {
        this.withdrawalAddresses = withdrawalAddresses;
    }

    public Map<String, Map<String, List<String>>> getSupportedNetworksFallback() {
        return supportedNetworksFallback;
    }

    public void setSupportedNetworksFallback(Map<String, Map<String, List<String>>> supportedNetworksFallback) {
        this.supportedNetworksFallback = supportedNetworksFallback;
    }

    public Map<String, Map<String, Map<String, String>>> getWithdrawFeesFallback() {
        return withdrawFeesFallback;
    }

    public void setWithdrawFeesFallback(Map<String, Map<String, Map<String, String>>> withdrawFeesFallback) {
        this.withdrawFeesFallback = withdrawFeesFallback;
    }
}

