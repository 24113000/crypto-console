package com.crypto.console.common.exchange;

import java.util.Set;

public interface DepositNetworkProvider {
    Set<String> getDepositNetworks(String asset);
}



