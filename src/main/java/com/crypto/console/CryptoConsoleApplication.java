package com.crypto.console;

import com.crypto.console.repl.ReplRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CryptoConsoleApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(CryptoConsoleApplication.class);
    private final ReplRunner replRunner;

    public CryptoConsoleApplication(ReplRunner replRunner) {
        this.replRunner = replRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(CryptoConsoleApplication.class, args);
    }

    @Override
    public void run(String... args) {
        try {
            replRunner.run();
        } catch (Exception e) {
            log.error("Startup failed: {}", e.getMessage());
            System.err.println("Startup failed: " + e.getMessage());
        }
    }
}


