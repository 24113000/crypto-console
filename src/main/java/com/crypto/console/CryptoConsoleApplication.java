package com.crypto.console;

import com.crypto.console.repl.ReplRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class CryptoConsoleApplication implements CommandLineRunner {
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
            LOG.error("Startup failed: {}", e.getMessage());
            System.err.println("Startup failed: " + e.getMessage());
        }
    }
}


