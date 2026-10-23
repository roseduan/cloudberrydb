/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.config;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads {@code application.properties} the way Spring Boot reads it, without
 * Spring.
 *
 * <p>"Both jars use java.util.Properties on the same file" is not enough for
 * the two to agree: the file is flat {@code key=value}, but its MEANING comes
 * from Spring. {@code dlagent.iceberg.hive-catalog-cache-ttl=5m} is a Duration,
 * {@code gopher.block-size=16777216} is a long, {@code gopher.cache-strategy}
 * binds to a camelCase field, {@code logging.file.name} contains a placeholder,
 * and {@code --gopher.enabled=false} on the command line has to beat the file.
 * Plain Properties returns the raw string for all of those and no precedence
 * at all.
 *
 * <p>Four Spring behaviours are reproduced, which is what the existing
 * configuration actually uses:
 *
 * <ol>
 * <li><b>Precedence</b>: command line &gt; environment &gt; file &gt; default.
 * <li><b>Relaxed binding</b>: {@code gopher.cache-strategy},
 *     {@code gopher.cacheStrategy} and {@code GOPHER_CACHE_STRATEGY} are the
 *     same property. Lookup normalises by lower-casing and dropping
 *     {@code -} and {@code _}, which is how the environment variable form is
 *     recognised too.
 * <li><b>Placeholders</b>: {@code ${name:default}}, resolved against the
 *     already-merged configuration and then the environment.
 * <li><b>Type conversion</b>: boolean, int, long and Duration, the last using
 *     Spring's suffixes ({@code ns us ms s m h d}) with milliseconds as the
 *     default unit.
 * </ol>
 *
 * <p>ONE SPRING BEHAVIOUR WORTH KNOWING ABOUT, reproduced deliberately: in
 * {@code ${A:B}} the part after the colon is a LITERAL default, not a second
 * variable to look up. So {@code logging.file.name=${COORDINATOR_DATA_DIRECTORY:MASTER_DATA_DIRECTORY}/log/dlagent.log}
 * resolves to a directory literally named {@code MASTER_DATA_DIRECTORY} when
 * that first variable is unset -- almost certainly not what whoever wrote that
 * line meant, but it is what the REST jar does today, and this parser exists to
 * agree with the REST jar rather than to improve on it. Fixing it would change
 * where the rollback path writes its log.
 */
public final class DlConfig {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^:}]+)(?::([^}]*))?}");

    /** Raw values, by normalised key. */
    private final Map<String, String> values;
    /** The key each value was last seen under, for reporting. */
    private final Map<String, String> originalKeys;
    private final Map<String, String> environment;

    private DlConfig(Map<String, String> values, Map<String, String> originalKeys,
                     Map<String, String> environment) {
        this.values = values;
        this.originalKeys = originalKeys;
        this.environment = environment;
    }

    /**
     * Builds the effective configuration.
     *
     * @param propertiesFile the packaged application.properties, may be null
     * @param environment    the process environment
     * @param args           command line arguments, {@code --key=value} form
     */
    public static DlConfig load(InputStream propertiesFile, Map<String, String> environment,
                                String[] args) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        Map<String, String> originalKeys = new LinkedHashMap<>();

        // Lowest precedence first, so later puts win.
        if (propertiesFile != null) {
            Properties p = new Properties();
            p.load(propertiesFile);
            rejectCollisions(p);
            for (String name : p.stringPropertyNames()) {
                put(values, originalKeys, name, p.getProperty(name));
            }
        }
        for (Map.Entry<String, String> e : environment.entrySet()) {
            // Only environment entries that look like a configuration key can
            // win over the file; otherwise PATH or HOME would shadow
            // properties with the same normalised name.
            if (values.containsKey(normalise(e.getKey()))) {
                put(values, originalKeys, e.getKey(), e.getValue());
            }
        }
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String body = arg.substring(2);
                int eq = body.indexOf('=');
                String key = eq < 0 ? body : body.substring(0, eq);
                String value = eq < 0 ? "true" : body.substring(eq + 1);
                put(values, originalKeys, key, value);
            }
        }
        return new DlConfig(values, originalKeys, environment);
    }

    private static void put(Map<String, String> values, Map<String, String> originalKeys,
                            String key, String value) {
        String n = normalise(key);
        values.put(n, value);
        // Keep the first spelling seen: the file's own key is the one an
        // operator recognises in a config dump, not the environment variable
        // that happened to override it.
        originalKeys.putIfAbsent(n, key);
    }

    /**
     * Fails if two properties in the FILE collapse to the same normalised key.
     *
     * <p>Only the file is checked: an environment variable or a command-line
     * argument matching an existing key is an override, which is the whole
     * point, whereas two different keys in one properties file becoming one is
     * a silent loss of configuration. Reporting it is cheap; discovering later
     * that half a setting was never read is not.
     */
    private static void rejectCollisions(Properties file) {
        Map<String, String> seen = new LinkedHashMap<>();
        for (String name : file.stringPropertyNames()) {
            String n = normalise(name);
            String previous = seen.put(n, name);
            if (previous != null && !previous.equals(name)) {
                throw new IllegalStateException(
                        "configuration keys '" + previous + "' and '" + name
                                + "' are indistinguishable under relaxed binding; one of "
                                + "them would be silently ignored");
            }
        }
    }

    /**
     * Relaxed binding: case and separators do not distinguish keys, so
     * {@code server.port}, {@code SERVER_PORT},
     * {@code gopher.cache-strategy}, {@code gopher.cacheStrategy} and
     * {@code GOPHER_CACHE_STRATEGY} each name one property.
     *
     * <p>All three separators are dropped rather than mapped onto each other,
     * because Spring's environment form uses {@code _} for BOTH {@code .} and
     * {@code -} ({@code gopher.cache-strategy} is looked up as
     * {@code GOPHER_CACHE_STRATEGY}), so no one-to-one substitution can make
     * the four spellings agree.
     *
     * <p>That is aggressive enough to fold two genuinely different keys
     * together -- {@code a.bc} and {@code ab.c} both become {@code abc} -- so
     * {@link #load} refuses to merge distinct original keys silently.
     */
    static String normalise(String key) {
        StringBuilder sb = new StringBuilder(key.length());
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c == '-' || c == '_' || c == '.') {
                continue;
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

    public String get(String key, String defaultValue) {
        String raw = values.get(normalise(key));
        return raw == null ? defaultValue : resolve(raw, 0);
    }

    public String get(String key) {
        return get(key, null);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String v = get(key);
        return v == null ? defaultValue : Boolean.parseBoolean(v.trim());
    }

    public int getInt(String key, int defaultValue) {
        String v = get(key);
        return v == null || v.trim().isEmpty() ? defaultValue : Integer.parseInt(v.trim());
    }

    public long getLong(String key, long defaultValue) {
        String v = get(key);
        return v == null || v.trim().isEmpty() ? defaultValue : Long.parseLong(v.trim());
    }

    /**
     * Spring's data-size syntax ({@code DataSize.parse}): a bare number is
     * bytes, or a number with one of B, KB, MB, GB, TB (binary multiples).
     */
    public long getDataSize(String key, long defaultBytes) {
        String v = get(key);
        if (v == null || v.trim().isEmpty()) {
            return defaultBytes;
        }
        Matcher m = Pattern.compile("^([+-]?\\d+)\\s*([a-zA-Z]*)$").matcher(v.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException("not a data size: '" + v + "'");
        }
        long n = Long.parseLong(m.group(1));
        switch (m.group(2).toUpperCase()) {
            case "":
            case "B":
                return n;
            case "KB":
                return n * 1024L;
            case "MB":
                return n * 1024L * 1024L;
            case "GB":
                return n * 1024L * 1024L * 1024L;
            case "TB":
                return n * 1024L * 1024L * 1024L * 1024L;
            default:
                throw new IllegalArgumentException("unknown data size unit '" + m.group(2)
                        + "' in '" + v + "'");
        }
    }

    /** Spring's duration syntax; a bare number is milliseconds. */
    public Duration getDuration(String key, Duration defaultValue) {
        String v = get(key);
        if (v == null || v.trim().isEmpty()) {
            return defaultValue;
        }
        return parseDuration(v.trim());
    }

    static Duration parseDuration(String text) {
        Matcher m = Pattern.compile("^([+-]?\\d+)\\s*([a-zA-Z]*)$").matcher(text);
        if (!m.matches()) {
            throw new IllegalArgumentException("not a duration: '" + text + "'");
        }
        long n = Long.parseLong(m.group(1));
        String unit = m.group(2).toLowerCase();
        switch (unit) {
            case "":
            case "ms":
                return Duration.ofMillis(n);
            case "ns":
                return Duration.ofNanos(n);
            case "us":
                return Duration.ofNanos(n * 1000L);
            case "s":
                return Duration.ofSeconds(n);
            case "m":
                return Duration.ofMinutes(n);
            case "h":
                return Duration.ofHours(n);
            case "d":
                return Duration.ofDays(n);
            default:
                throw new IllegalArgumentException("unknown duration unit '" + unit
                        + "' in '" + text + "'");
        }
    }

    private String resolve(String raw, int depth) {
        if (depth > 8) {
            throw new IllegalStateException("placeholder recursion too deep in '" + raw + "'");
        }
        Matcher m = PLACEHOLDER.matcher(raw);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String name = m.group(1);
            String fallback = m.group(2);
            String v = values.get(normalise(name));
            if (v == null) {
                v = environment.get(name);
            }
            if (v == null) {
                // Spring treats the text after ':' as a literal, not as another
                // lookup. See the class javadoc.
                v = fallback == null ? m.group(0) : fallback;
            } else {
                v = resolve(v, depth + 1);
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(v));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * Every key with its resolved value, sorted, for the cross-jar comparison:
     * start both jars with the same files, environment and arguments, dump
     * this, and diff.
     */
    public Map<String, String> effectiveConfiguration() {
        Map<String, String> out = new TreeMap<>();
        for (Map.Entry<String, String> e : values.entrySet()) {
            out.put(originalKeys.get(e.getKey()), resolve(e.getValue(), 0));
        }
        return Collections.unmodifiableMap(out);
    }
}
