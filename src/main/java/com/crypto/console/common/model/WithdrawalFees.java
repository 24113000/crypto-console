package com.crypto.console.common.model;

import java.math.BigDecimal;
import java.util.Map;

public class WithdrawalFees {
    public final String asset;
    public final Map<String, BigDecimal> feeByNetwork;

    public WithdrawalFees(String asset, Map<String, BigDecimal> feeByNetwork) {
        this.asset = asset;
        this.feeByNetwork = feeByNetwork;
    }
}



