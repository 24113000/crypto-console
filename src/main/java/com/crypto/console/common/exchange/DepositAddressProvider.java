package com.crypto.console.common.exchange;

public interface DepositAddressProvider {
    String getDepositAddress(String asset, String network);
}
