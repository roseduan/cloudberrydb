package org.apache.cloudberry.iceberg.gateway.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Immutable runtime configuration, loaded from {@code gateway.properties} on the classpath
 * and overridable via environment variables (e.g. {@code PG_JDBCURL} overrides {@code pg.jdbcUrl}).
 *
 * <p>See {@code src/main/resources/gateway.properties} for the full key set.
 */
public final class GatewayConfig {

    private final Properties props;

    private GatewayConfig(Properties props) {
        this.props = props;
    }

    /** Load defaults from the bundled properties file, then overlay environment variables. */
    public static GatewayConfig load() {
        Properties p = new Properties();
        try (InputStream in = GatewayConfig.class.getResourceAsStream("/gateway.properties")) {
            if (in != null) {
                p.load(in);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read gateway.properties", e);
        }
        // Env override: pg.jdbcUrl -> PG_JDBCURL, s3.access-key-id -> S3_ACCESS_KEY_ID
        for (String key : p.stringPropertyNames()) {
            String env = key.toUpperCase().replace('.', '_').replace('-', '_');
            String v = System.getenv(env);
            if (v != null && !v.isBlank()) {
                p.setProperty(key, v);
            }
        }
        return new GatewayConfig(p);
    }

    public String get(String key) {
        return props.getProperty(key);
    }

    public String get(String key, String def) {
        return props.getProperty(key, def);
    }

    public int getInt(String key, int def) {
        String v = props.getProperty(key);
        return (v == null || v.isBlank()) ? def : Integer.parseInt(v.trim());
    }

    public boolean getBool(String key, boolean def) {
        String v = props.getProperty(key);
        return (v == null || v.isBlank()) ? def : Boolean.parseBoolean(v.trim());
    }

    /** Test-only: build config from an explicit map overlaid on the bundled defaults. */
    public static GatewayConfig forTest(java.util.Map<String,String> overrides) {
        GatewayConfig base = load();
        java.util.Properties p = new java.util.Properties();
        p.putAll(base.props);
        p.putAll(overrides);
        return new GatewayConfig(p);
    }
}
