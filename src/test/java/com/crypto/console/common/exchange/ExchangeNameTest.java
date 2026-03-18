package com.crypto.console.common.exchange;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExchangeNameTest {

    @Test
    void resolvesThreeCharacterAliases() {
        assertEquals(ExchangeName.ASCENDEX, ExchangeName.from("asc"));
        assertEquals(ExchangeName.BINANCE, ExchangeName.from("bin"));
        assertEquals(ExchangeName.BYBIT, ExchangeName.from("byb"));
        assertEquals(ExchangeName.GATEIO, ExchangeName.from("gio"));
        assertEquals(ExchangeName.KUCOIN, ExchangeName.from("kuc"));
    }

    @Test
    void resolvesLegacyAliases() {
        assertEquals(ExchangeName.ASCENDEX, ExchangeName.from("ascend"));
        assertEquals(ExchangeName.GATEIO, ExchangeName.from("gate"));
        assertEquals(ExchangeName.BITRUE, ExchangeName.from("bittrue"));
    }
}
