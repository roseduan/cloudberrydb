/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.grpc.config.ConfigurationBinder;
import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two jars must read the same configuration out of the same inputs.
 *
 * <p>This is the check the design asked for in so many words -- "start
 * both jars with the same files, environment and arguments, dump the effective
 * configuration, and diff" -- and which was never written. Its absence cost:
 * the gRPC agent shipped running on NONE of the packaged configuration, so
 * {@code gopher.enabled} was false there and true under REST, which is a
 * different FileIO and therefore a different code path for every catalog
 * operation. Three separate mistakes had to line up for that, and a dump-and-
 * diff would have caught any one of them.
 *
 * <p>The REST side here is Spring's own {@code StandardEnvironment} with the
 * property sources Spring Boot installs, in Spring Boot's order; the gRPC side
 * is {@link DlConfig}. Neither is reimplemented -- the test only feeds both the
 * same three inputs and compares what they answer.
 */
class ConfigParityTest {

    private static final String FILE = "/application.properties";

    private static Properties packaged() throws Exception {
        Properties p = new Properties();
        try (InputStream in = ConfigParityTest.class.getResourceAsStream(FILE)) {
            assertNotNull(in, "application.properties must be on the classpath");
            p.load(in);
        }
        return p;
    }

    /** Spring's view, with the sources spring-boot installs and in its order. */
    private static StandardEnvironment springEnvironment(Map<String, Object> env, String... args) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().remove(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(
                StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        try {
            // Lowest precedence first, then override: file < environment < command line.
            environment.getPropertySources().addLast(
                    new PropertiesPropertySource("applicationConfig", packaged()));
            environment.getPropertySources().addFirst(
                    new SystemEnvironmentPropertySource("systemEnvironment", env));
            if (args.length > 0) {
                environment.getPropertySources().addFirst(
                        new SimpleCommandLinePropertySource(args));
            }
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return environment;
    }

    private static DlConfig grpcConfig(Map<String, String> env, String... args) throws Exception {
        try (InputStream in = ConfigParityTest.class.getResourceAsStream(FILE)) {
            return DlConfig.load(in, env, args);
        }
    }

    @Test
    @DisplayName("every key in the packaged file resolves to the same value on both")
    void everyPackagedKeyAgrees() throws Exception {
        Properties file = packaged();
        StandardEnvironment spring = springEnvironment(Collections.emptyMap());
        DlConfig grpc = grpcConfig(Collections.emptyMap());

        Map<String, String> differences = new TreeMap<>();
        for (String key : file.stringPropertyNames()) {
            String springValue = spring.getProperty(key);
            String grpcValue = grpc.get(key);
            if (!java.util.Objects.equals(springValue, grpcValue)) {
                differences.put(key, "spring='" + springValue + "' grpc='" + grpcValue + "'");
            }
        }
        assertTrue(file.size() > 30,
                "expected the whole packaged file, saw " + file.size() + " keys");
        assertTrue(differences.isEmpty(),
                "the two jars read these keys differently:\n  "
                        + String.join("\n  ", differences.entrySet().stream()
                                .map(e -> e.getKey() + ": " + e.getValue())
                                .toArray(String[]::new)));
    }

    @Test
    @DisplayName("a command-line argument beats the file on both")
    void commandLineWins() throws Exception {
        StandardEnvironment spring =
                springEnvironment(Collections.emptyMap(), "--gopher.enabled=false");
        DlConfig grpc = grpcConfig(Collections.emptyMap(), "--gopher.enabled=false");

        assertEquals("false", spring.getProperty("gopher.enabled"));
        assertEquals("false", grpc.get("gopher.enabled"));
    }

    @Test
    @DisplayName("an environment variable beats the file on both, in its SCREAMING_CASE spelling")
    void environmentWins() throws Exception {
        Map<String, Object> springEnv = new LinkedHashMap<>();
        springEnv.put("GOPHER_CACHE_STRATEGY", "NO_CACHE");
        Map<String, String> grpcEnv = Collections.singletonMap("GOPHER_CACHE_STRATEGY", "NO_CACHE");

        assertEquals("NO_CACHE",
                springEnvironment(springEnv).getProperty("gopher.cache-strategy"),
                "spring's relaxed binding should see the environment spelling");
        assertEquals("NO_CACHE", grpcConfig(grpcEnv).get("gopher.cache-strategy"),
                "DlConfig's relaxed binding should see the same");
    }

    @Test
    @DisplayName("a key nobody set is absent on both, not empty-string on one")
    void absentKey() throws Exception {
        StandardEnvironment spring = springEnvironment(Collections.emptyMap());
        DlConfig grpc = grpcConfig(Collections.emptyMap());
        assertEquals(null, spring.getProperty("dlagent.no.such.key"));
        assertEquals(null, grpc.get("dlagent.no.such.key"));
    }

    @Test
    @DisplayName("the gopher settings the agent actually binds come from the file, not the class")
    void gopherPropertiesAreBound() throws Exception {
        GopherConfigurationProperties defaults = new GopherConfigurationProperties();
        GopherConfigurationProperties bound = new GopherConfigurationProperties();
        ConfigurationBinder.bind(bound, GopherConfigurationProperties.PROPERTY_PREFIX,
                                 grpcConfig(Collections.emptyMap()));

        assertEquals(Boolean.FALSE, defaults.getEnabled(),
                "if the class default ever equals the shipped value this test stops proving "
                        + "anything");
        assertEquals("true", packaged().getProperty("gopher.enabled"));
        assertEquals(Boolean.TRUE, bound.getEnabled(),
                "the gRPC agent must end up with the file's value, which is the whole point");
    }

    @Test
    @DisplayName("the two placeholder spellings resolve the same way")
    void placeholders() throws Exception {
        /*
         * logging.file.name is
         *   ${COORDINATOR_DATA_DIRECTORY:MASTER_DATA_DIRECTORY}/log/dlagent.log
         * and in ${A:B} the B is a LITERAL default, not a second variable. So
         * with COORDINATOR_DATA_DIRECTORY unset both sides must produce a path
         * under a directory literally named MASTER_DATA_DIRECTORY. That is
         * almost certainly not what whoever wrote the line meant, but it is
         * what the REST jar does today, and parity means the gRPC jar has to be
         * wrong in exactly the same way until someone fixes the line.
         */
        StandardEnvironment spring = springEnvironment(Collections.emptyMap());
        DlConfig grpc = grpcConfig(Collections.emptyMap());
        assertEquals(spring.getProperty("logging.file.name"), grpc.get("logging.file.name"));

        Map<String, Object> springEnv =
                Collections.singletonMap("COORDINATOR_DATA_DIRECTORY", "/data/qd");
        Map<String, String> grpcEnv =
                Collections.singletonMap("COORDINATOR_DATA_DIRECTORY", "/data/qd");
        assertEquals(springEnvironment(springEnv).getProperty("logging.file.name"),
                     grpcConfig(grpcEnv).get("logging.file.name"));
    }

    @Test
    @DisplayName("the effective dump is complete enough to diff two jars with")
    void effectiveConfigurationIsDumpable() throws Exception {
        Map<String, String> dump = grpcConfig(Collections.emptyMap()).effectiveConfiguration();
        Properties file = packaged();
        for (String key : file.stringPropertyNames()) {
            assertTrue(dump.containsKey(key), "the dump is missing " + key);
        }
    }

    /**
     * The gRPC agent's log4j2-dlagent.xml is REST's with three lines changed
     * for the absence of Spring (the level lookup, %xwEx, and the layout's
     * alwaysWriteExceptions) and an explanatory comment. Anything else that
     * differs would make the two agents log differently.
     */
    @Test
    @DisplayName("the two log4j2-dlagent.xml files differ only where Spring is missing")
    void loggingConfigurationsAgree() throws Exception {
        java.util.List<String> rest = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(
                "../dlagent-rest/src/main/resources/log4j2-dlagent.xml"));
        String grpcText = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(
                "../dlagent-grpc/src/main/resources/log4j2-dlagent.xml")),
                java.nio.charset.StandardCharsets.UTF_8);
        // Drop the explanatory comment, which REST's file does not have.
        grpcText = grpcText.replaceFirst("(?s)<!--.*?-->\\n", "");
        java.util.List<String> grpc = java.util.Arrays.asList(grpcText.split("\n", -1));
        if (grpc.get(grpc.size() - 1).isEmpty() && !rest.isEmpty()) {
            grpc = grpc.subList(0, grpc.size() - 1);
        }

        Map<String, String> allowed = new LinkedHashMap<>();
        allowed.put("    <Property name=\"LOG_EXCEPTION_CONVERSION_WORD\">%xwEx</Property>",
                "    <Property name=\"LOG_EXCEPTION_CONVERSION_WORD\">%notEmpty{%n%xEx%n}</Property>");
        allowed.put("        <PatternLayout pattern=\"${sys:FILE_LOG_PATTERN}\" "
                        + "charset=\"${sys:FILE_LOG_CHARSET}\"/>",
                "        <PatternLayout pattern=\"${sys:FILE_LOG_PATTERN}\" "
                        + "charset=\"${sys:FILE_LOG_CHARSET}\" alwaysWriteExceptions=\"false\"/>");
        allowed.put("    <Logger name=\"cloud.elastic.dlagent\" level=\"${spring:dlagent.log.level}\"/>",
                "    <Logger name=\"cloud.elastic.dlagent\" level=\"${sys:dlagent.log.level:-info}\"/>");

        assertEquals(rest.size(), grpc.size(), "the files no longer line up:\n" + grpcText);
        int changed = 0;
        for (int i = 0; i < rest.size(); i++) {
            if (rest.get(i).equals(grpc.get(i))) {
                continue;
            }
            assertEquals(allowed.get(rest.get(i)), grpc.get(i),
                    "line " + (i + 1) + " differs beyond what the absence of Spring needs");
            changed++;
        }
        assertEquals(allowed.size(), changed, "an expected difference is missing");
    }

    /** Silences an unused-import warning while keeping the source list explicit. */
    @SuppressWarnings("unused")
    private static final Class<?> KEEP = MapPropertySource.class;
}
