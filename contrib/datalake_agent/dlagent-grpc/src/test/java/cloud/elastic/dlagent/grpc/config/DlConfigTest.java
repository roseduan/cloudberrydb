/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.config;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The configuration semantics the REST jar gets from Spring and this parser has
 * to reproduce. Every case below is taken from the real
 * {@code application.properties} rather than invented.
 */
public class DlConfigTest {

    private DlConfig load(String properties, Map<String, String> env, String... args)
            throws Exception {
        return DlConfig.load(
                new ByteArrayInputStream(properties.getBytes(StandardCharsets.UTF_8)),
                env, args);
    }

    /**
     * The one override datalake_proxy.c passes ({@code --gopher.enabled=false}),
     * and one that may never stop working: an open-source build turns
     * GopherFileIO off this way, and if the override is ignored every catalog
     * metadata operation goes to a gopher socket that does not exist.
     */
    @Test
    public void commandLineBeatsTheFile() throws Exception {
        DlConfig c = load("gopher.enabled=true\n", Collections.emptyMap(),
                "--gopher.enabled=false");
        assertEquals(false, c.getBoolean("gopher.enabled", true));
    }

    @Test
    public void environmentBeatsTheFileAndLosesToTheCommandLine() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("SERVER_PORT", "4888");
        assertEquals(4888, load("server.port=3888\n", env).getInt("server.port", 0));
        assertEquals(5888, load("server.port=3888\n", env, "--server.port=5888")
                .getInt("server.port", 0));
    }

    /**
     * An environment variable that does not name a known property must not
     * shadow one by accident; PATH and HOME are always present.
     */
    @Test
    public void unrelatedEnvironmentEntriesAreIgnored() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("PATH", "/usr/bin");
        env.put("HOME", "/root");
        DlConfig c = load("server.port=3888\n", env);
        assertEquals(3888, c.getInt("server.port", 0));
        assertEquals(1, c.effectiveConfiguration().size(),
                "only declared properties belong in the effective configuration");
    }

    /** gopher.cache-strategy, gopher.cacheStrategy and GOPHER_CACHE_STRATEGY are one key. */
    @Test
    public void relaxedBinding() throws Exception {
        DlConfig c = load("gopher.cache-strategy=GOPHER_CACHE\n", Collections.emptyMap());
        assertEquals("GOPHER_CACHE", c.get("gopher.cache-strategy"));
        assertEquals("GOPHER_CACHE", c.get("gopher.cacheStrategy"));
        assertEquals("GOPHER_CACHE", c.get("GOPHER_CACHE_STRATEGY"));
    }

    /** dlagent.iceberg.hive-catalog-cache-ttl=5m is a Duration, not the string "5m". */
    @Test
    public void durationsUseSpringSuffixes() throws Exception {
        DlConfig c = load("dlagent.iceberg.hive-catalog-cache-ttl=5m\n"
                + "server.tomcat.connection-timeout=5m\n"
                + "spring.mvc.async.request-timeout=86400000\n", Collections.emptyMap());
        assertEquals(Duration.ofMinutes(5),
                c.getDuration("dlagent.iceberg.hive-catalog-cache-ttl", null));
        assertEquals(Duration.ofMinutes(5),
                c.getDuration("server.tomcat.connection-timeout", null));
        // A bare number is milliseconds, as in Spring.
        assertEquals(Duration.ofMillis(86400000L),
                c.getDuration("spring.mvc.async.request-timeout", null));

        assertEquals(Duration.ofSeconds(30), DlConfig.parseDuration("30s"));
        assertEquals(Duration.ofHours(2), DlConfig.parseDuration("2h"));
        assertEquals(Duration.ofDays(1), DlConfig.parseDuration("1d"));
        assertThrows(IllegalArgumentException.class, () -> DlConfig.parseDuration("5 fortnights"));
    }

    @Test
    public void numbersAndBooleans() throws Exception {
        DlConfig c = load("gopher.block-size=16777216\ngopher.buffer-size=16384\n"
                + "dlagent.metrics.partition.enabled=true\n"
                + "dlagent.task.pool.core-size=8\n", Collections.emptyMap());
        assertEquals(16777216L, c.getLong("gopher.block-size", 0));
        assertEquals(16384, c.getInt("gopher.buffer-size", 0));
        assertEquals(8, c.getInt("dlagent.task.pool.core-size", 0));
        assertTrue(c.getBoolean("dlagent.metrics.partition.enabled", false));
    }

    /**
     * The placeholder from the real file, with the surprise spelled out: in
     * Spring, {@code ${A:B}} falls back to the LITERAL text B, not to a second
     * variable named B. Reproducing that is the point -- the two jars have to
     * agree, and the REST jar behaves this way today.
     */
    @Test
    public void placeholderFallbackIsALiteralJustAsInSpring() throws Exception {
        String props = "logging.file.name=${COORDINATOR_DATA_DIRECTORY:MASTER_DATA_DIRECTORY}"
                + "/log/dlagent.log\n";

        Map<String, String> set = new HashMap<>();
        set.put("COORDINATOR_DATA_DIRECTORY", "/data/coord");
        assertEquals("/data/coord/log/dlagent.log",
                load(props, set).get("logging.file.name"));

        // Unset: the literal, NOT the value of a variable called
        // MASTER_DATA_DIRECTORY even when that variable exists.
        Map<String, String> other = new HashMap<>();
        other.put("MASTER_DATA_DIRECTORY", "/data/master");
        assertEquals("MASTER_DATA_DIRECTORY/log/dlagent.log",
                load(props, other).get("logging.file.name"),
                "Spring resolves the text after ':' as a literal; matching that is what "
                        + "keeps the two jars writing to the same place");
    }

    /** A placeholder naming another property resolves transitively. */
    @Test
    public void placeholdersResolveAgainstOtherProperties() throws Exception {
        DlConfig c = load("a=hello\nb=${a} world\n", Collections.emptyMap());
        assertEquals("hello world", c.get("b"));
    }

    /**
     * Relaxed binding folds separators away, which is aggressive enough that
     * two different keys could become one. That must be an error, not a silent
     * loss of whichever lost the race.
     */
    @Test
    public void indistinguishableKeysInTheFileAreRejected() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> load("a.bc=1\nab.c=2\n", Collections.emptyMap()));
        assertTrue(e.getMessage().contains("indistinguishable"), e.getMessage());
    }

    /**
     * The real file must be free of such collisions.
     *
     * <p>Read from the CLASSPATH, not from a source path: the packaged file is
     * the one both jars actually load, and reading it this way also fails if it
     * ever stops being packaged where the gRPC agent can see it -- which is
     * exactly what happened while it lived in dlagent-rest's resources.
     */
    @Test
    public void theShippedPropertiesFileHasNoCollisions() throws Exception {
        try (java.io.InputStream in =
                     DlConfig.class.getResourceAsStream("/application.properties")) {
            assertTrue(in != null, "application.properties is not on the gRPC classpath");
            DlConfig c = DlConfig.load(in, Collections.emptyMap(), new String[0]);
            assertTrue(c.effectiveConfiguration().size() > 30,
                    "expected the whole file, saw " + c.effectiveConfiguration().size() + " keys");
        }
    }

    /**
     * The dump both jars have to be able to produce so their effective
     * configurations can be diffed key by key.
     */
    @Test
    public void effectiveConfigurationIsDumpable() throws Exception {
        DlConfig c = load("server.port=3888\ngopher.enabled=true\n", Collections.emptyMap(),
                "--gopher.enabled=false", "--parent.pid=1234");
        Map<String, String> dump = c.effectiveConfiguration();
        assertEquals("3888", dump.get("server.port"));
        assertEquals("false", dump.get("gopher.enabled"));
        assertEquals("1234", dump.get("parent.pid"));
    }
}
