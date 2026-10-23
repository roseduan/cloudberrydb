/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.config;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;

/**
 * Fills a configuration object from {@link DlConfig}, the way Spring Boot's
 * {@code @ConfigurationProperties} fills the same object on the REST side.
 *
 * <p>WHY THIS EXISTS. {@code GopherConfigurationProperties} and
 * {@code DlServerProperties} are plain POJOs whose field initialisers are
 * *fallbacks*, not the shipped configuration: {@code gopher.enabled} is
 * {@code false} in the class and {@code true} in application.properties, and
 * the block and buffer sizes differ by a factor of eight. Under REST, Spring
 * binds the file over those fallbacks. Without an equivalent step the gRPC
 * agent would run with gopher off while the REST agent runs with it on, which
 * is a different FileIO, a different code path and different bytes on the wire
 * -- exactly the kind of divergence between the two transports that must not
 * happen.
 *
 * <p>WHAT IS REPRODUCED. Only what the shipped configuration actually uses:
 * String, boolean, int, long and Duration leaves, plus recursion into nested
 * configuration objects ({@code dlagent.task.pool.core-size}). Key lookup,
 * relaxed binding and type conversion are {@link DlConfig}'s, so a property
 * resolves here exactly as it resolves anywhere else in this jar.
 *
 * <p>A nested object is recursed into rather than constructed: several are
 * {@code final} fields with no setter ({@code TaskProperties.pool}), so the
 * instance the POJO already holds is the only one there will ever be.
 */
public final class ConfigurationBinder {

    private ConfigurationBinder() {
    }

    /**
     * Binds every property under {@code prefix} into {@code target}.
     *
     * @param target the configuration object to fill, in place
     * @param prefix the property prefix, e.g. {@code "gopher"}
     * @param config the resolved configuration
     */
    public static void bind(Object target, String prefix, DlConfig config) {
        for (Field field : target.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            String key = prefix + "." + kebab(field.getName());
            Class<?> type = field.getType();

            try {
                if (isLeaf(type)) {
                    Object value = read(config, key, type, currentValue(target, field));
                    if (value != null) {
                        assign(target, field, value);
                    }
                }
                else if (isNested(type)) {
                    Object nested = currentValue(target, field);
                    if (nested != null) {
                        bind(nested, key, config);
                    }
                }
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException(
                        "could not bind configuration property '" + key + "'", e);
            }
        }
    }

    /**
     * Only types the shipped configuration actually contains. Anything else is
     * left alone rather than guessed at: a silently unbound property is found
     * by the effective-configuration dump, a wrongly converted one is not.
     */
    private static boolean isLeaf(Class<?> type) {
        return type == String.class
                || type == boolean.class || type == Boolean.class
                || type == int.class || type == Integer.class
                || type == long.class || type == Long.class
                || type == Duration.class;
    }

    /**
     * A nested configuration object is one of ours; recursing into anything
     * else (a Map, a List, a JDK type) would be a guess about its shape.
     */
    private static boolean isNested(Class<?> type) {
        return type.getName().startsWith("cloud.elastic.dlagent.");
    }

    private static Object read(DlConfig config, String key, Class<?> type, Object fallback) {
        if (config.get(key) == null) {
            return null;
        }
        if (type == String.class) {
            return config.get(key);
        }
        if (type == boolean.class || type == Boolean.class) {
            return config.getBoolean(key, fallback instanceof Boolean && (Boolean) fallback);
        }
        if (type == int.class || type == Integer.class) {
            return config.getInt(key, fallback instanceof Integer ? (Integer) fallback : 0);
        }
        if (type == long.class || type == Long.class) {
            return config.getLong(key, fallback instanceof Long ? (Long) fallback : 0L);
        }
        return config.getDuration(key, fallback instanceof Duration ? (Duration) fallback : null);
    }

    /**
     * Prefer the setter, because a POJO may do something in it; fall back to
     * the field for the few that have none.
     */
    private static void assign(Object target, Field field, Object value)
            throws ReflectiveOperationException {
        String setter = "set" + Character.toUpperCase(field.getName().charAt(0))
                + field.getName().substring(1);
        try {
            Method m = target.getClass().getMethod(setter, field.getType());
            m.invoke(target, value);
            return;
        }
        catch (NoSuchMethodException ignored) {
            // final field, or a POJO without lombok's @Setter
        }

        if (Modifier.isFinal(field.getModifiers())) {
            return;
        }
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object currentValue(Object target, Field field)
            throws ReflectiveOperationException {
        field.setAccessible(true);
        return field.get(target);
    }

    /** {@code blockSize} -> {@code block-size}, the spelling in the file. */
    private static String kebab(String name) {
        StringBuilder out = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c)) {
                out.append('-').append(Character.toLowerCase(c));
            }
            else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
