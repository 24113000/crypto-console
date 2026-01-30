package com.crypto.console.common.service;

import com.crypto.console.common.model.ExchangeException;
import com.crypto.console.common.util.ConsoleOutput;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class NetworkSelector {
    private final Terminal terminal;

    public NetworkSelector() {
        this.terminal = buildTerminal();
    }

    public String selectNetwork(String exchange, String asset, List<String> networks) {
        if (networks == null || networks.isEmpty()) {
            throw new ExchangeException("No deposit networks available for " + asset);
        }
        if (terminal == null) {
            printFallback(exchange, asset, networks);
            return networks.getFirst();
        }

        PrintWriter out = terminal.writer();
        out.println(ConsoleOutput.green("Select deposit network for " + exchange + " " + asset + " (use arrows, Enter to confirm):"));
        out.flush();

        int selected = 0;
        Attributes previous = null;
        java.io.Reader reader = null;
        try {
            previous = terminal.enterRawMode();
            reader = terminal.reader();
            renderMenu(out, networks, selected);
            while (true) {
                int ch = reader.read();
                if (ch == -1) {
                    break;
                }
                if (ch == '\r' || ch == '\n') {
                    break;
                }
                if (ch == 'w' || ch == 'W') {
                    selected = (selected - 1 + networks.size()) % networks.size();
                    renderSelectionLine(out, networks, selected);
                    continue;
                }
                if (ch == 's' || ch == 'S') {
                    selected = (selected + 1) % networks.size();
                    renderSelectionLine(out, networks, selected);
                    continue;
                }
                if (ch >= '1' && ch <= '9') {
                    int idx = ch - '1';
                    if (idx >= 0 && idx < networks.size()) {
                        selected = idx;
                        renderSelectionLine(out, networks, selected);
                    }
                    continue;
                }
                if (ch == 27) {
                    int ch2 = reader.read();
                    if (ch2 != '[' && ch2 != 'O') {
                        continue;
                    }
                    int ch3 = reader.read();
                    if (ch3 == -1) {
                        continue;
                    }
                    if (ch3 == 'A') {
                        selected = (selected - 1 + networks.size()) % networks.size();
                        renderSelectionLine(out, networks, selected);
                    } else if (ch3 == 'B') {
                        selected = (selected + 1) % networks.size();
                        renderSelectionLine(out, networks, selected);
                    } else {
                        int last = ch3;
                        while ((last >= '0' && last <= '9') || last == ';') {
                            last = reader.read();
                            if (last == -1) {
                                break;
                            }
                        }
                        if (last == 'A') {
                            selected = (selected - 1 + networks.size()) % networks.size();
                            renderSelectionLine(out, networks, selected);
                        } else if (last == 'B') {
                            selected = (selected + 1) % networks.size();
                            renderSelectionLine(out, networks, selected);
                        }
                    }
                } else if (ch == 224 || ch == 0) {
                    int ch2 = reader.read();
                    if (ch2 == 72) {
                        selected = (selected - 1 + networks.size()) % networks.size();
                        renderSelectionLine(out, networks, selected);
                    } else if (ch2 == 80) {
                        selected = (selected + 1) % networks.size();
                        renderSelectionLine(out, networks, selected);
                    }
                }
            }
        } catch (Exception e) {
            printFallback(exchange, asset, networks);
            return networks.getFirst();
        } finally {
            if (previous != null) {
                terminal.setAttributes(previous);
            }
        }

        out.println();
        out.flush();
        return networks.get(selected);
    }

    private Terminal buildTerminal() {
        try {
            return TerminalBuilder.builder()
                    .system(true)
                    .jna(true)
                    .jansi(true)
                    .build();
        } catch (IOException e) {
            return null;
        }
    }

    private void renderMenu(PrintWriter out, List<String> networks, int selected) {
        for (String network : networks) {
            out.println(ConsoleOutput.green(" " + network));
        }
        out.print(selectionLine(networks.get(selected)));
        out.flush();
    }

    private void renderSelectionLine(PrintWriter out, List<String> networks, int selected) {
        clearLine(out);
        out.print(selectionLine(networks.get(selected)));
        out.flush();
    }

    private String selectionLine(String selected) {
        return ConsoleOutput.green("Selected: " + selected + " (Enter to confirm)");
    }

    private void printFallback(String exchange, String asset, List<String> networks) {
        ConsoleOutput.printlnGreen("Available deposit networks for " + exchange + " " + asset + ":");
        for (int i = 0; i < networks.size(); i++) {
            ConsoleOutput.printlnGreen("  " + (i + 1) + ") " + networks.get(i));
        }
        ConsoleOutput.printlnGreen("Interactive selection unavailable; defaulting to " + networks.getFirst() + ".");
    }

    private void clearLine(PrintWriter out) {
        out.print("\r\u001B[2K");
    }

}
