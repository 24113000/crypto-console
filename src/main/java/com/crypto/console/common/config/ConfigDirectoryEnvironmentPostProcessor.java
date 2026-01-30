package com.crypto.console.common.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.PropertySourceLoader;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class ConfigDirectoryEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {
    private static final String CONFIG_DIR_PROP = "app.config.dir";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String configDir = environment.getProperty(CONFIG_DIR_PROP);
        if (configDir == null || configDir.isBlank()) {
            return;
        }
        Path root = Path.of(configDir);
        if (!Files.exists(root) || !Files.isDirectory(root)) {
            LOG.warn("Config directory not found: {}", root.toAbsolutePath());
            return;
        }

        MutablePropertySources sources = environment.getPropertySources();
        PropertySourceLoader loader = new YamlPropertySourceLoader();

        loadYamlIfExists(loader, sources, root.resolve("base.yml"));
        loadYamlIfExists(loader, sources, root.resolve("base.yaml"));

        loadAllFromDir(loader, sources, root.resolve("exchanges"));
        loadAllFromDir(loader, sources, root.resolve("secrets"));
    }

    private void loadAllFromDir(PropertySourceLoader loader, MutablePropertySources sources, Path dir) {
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> stream = Files.list(dir)) {
            List<Path> files = stream
                    .filter(path -> Files.isRegularFile(path) && isYaml(path))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString().toLowerCase()))
                    .toList();
            for (Path file : files) {
                loadYamlIfExists(loader, sources, file);
            }
        } catch (IOException e) {
            LOG.warn("Failed to list config directory: {}", dir.toAbsolutePath());
        }
    }

    private void loadYamlIfExists(PropertySourceLoader loader, MutablePropertySources sources, Path path) {
        if (!Files.exists(path) || !Files.isRegularFile(path)) {
            return;
        }
        Resource resource = new FileSystemResource(path);
        try {
            List<PropertySource<?>> loaded = loader.load(path.toString(), resource);
            for (PropertySource<?> source : loaded) {
                sources.addLast(source);
                LOG.info("Loaded config: {}", path.toAbsolutePath());
            }
        } catch (IOException e) {
            LOG.warn("Failed to load config file: {}", path.toAbsolutePath());
        }
    }

    private boolean isYaml(Path path) {
        String name = path.getFileName().toString().toLowerCase();
        return name.endsWith(".yml") || name.endsWith(".yaml");
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
