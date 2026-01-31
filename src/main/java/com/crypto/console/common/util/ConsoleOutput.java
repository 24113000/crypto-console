package com.crypto.console.common.util;

public final class ConsoleOutput {
    private static final String GREEN = "\u001B[32m";
    private static final String BLUE = "\u001B[34m";
    private static final String RESET = "\u001B[0m";

    private ConsoleOutput() {
    }

    public static String green(String text) {
        return GREEN + text + RESET;
    }

    public static void printGreen(String text) {
        System.out.print(green(text));
    }

    public static void printlnGreen(String text) {
        System.out.println(green(text));
    }

    public static String blue(String text) {
        return BLUE + text + RESET;
    }

    public static void printBlue(String text) {
        System.out.print(blue(text));
    }
}
