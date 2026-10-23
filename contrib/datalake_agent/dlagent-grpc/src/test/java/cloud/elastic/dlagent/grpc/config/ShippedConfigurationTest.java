/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.config;

import cloud.elastic.dlagent.api.configuration.DlServerProperties;
import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.grpc.DiModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shipped configuration reaches the gRPC agent's objects.
 *
 * <p>Two failures this pins, both found by running the agent rather than by
 * reading it:
 *
 * <ol>
 * <li>application.properties packaged in dlagent-rest only (it lives in
 *     dlagent-core) is not on the gRPC classpath at all, and
 *     {@code getResourceAsStream} returns null.
 * <li>{@code Main} building {@code new DiModule()} -- an EMPTY configuration
 *     -- means that even with the file present the property objects keep
 *     their field initialisers.
 * </ol>
 *
 * <p>Either one alone leaves {@code gopher.enabled} at the class default
 * {@code false} while the REST agent reads {@code true} from the file: a
 * different FileIO, and two transports that do not behave alike.
 *
 * <p>The logging configuration is the one file that is NOT shared:
 * dlagent-rest's log4j2-dlagent.xml reads {@code ${spring:...}}, which only a
 * Spring environment can answer, so dlagent-grpc ships its own copy that
 * reads the same values from system properties {@code Main} sets the way
 * spring-boot does. ConfigParityTest holds the two files to that one
 * difference.
 */
class ShippedConfigurationTest {

    private static DlConfig shipped() throws Exception {
        try (InputStream in = DlConfig.class.getResourceAsStream("/application.properties")) {
            assertNotNull(in, "application.properties must be on the gRPC classpath");
            return DlConfig.load(in, Collections.emptyMap(), new String[0]);
        }
    }

    @Test
    @DisplayName("application.properties is packaged where both transports can read it")
    void packagedOnTheClasspath() throws Exception {
        DlConfig config = shipped();
        assertEquals("true", config.get("gopher.enabled"));
        assertEquals("localhost", config.get("server.address"));
    }

    @Test
    @DisplayName("the log4j2 configuration is on the gRPC classpath and needs no Spring")
    void loggingConfigurationIsPackaged() throws Exception {
        String xml;
        try (InputStream in = DlConfig.class.getResourceAsStream("/log4j2-dlagent.xml")) {
            assertNotNull(in, "log4j2-dlagent.xml must be on the gRPC classpath, or the "
                    + "agent writes no dlagent.log");
            xml = new String(org.apache.commons.io.IOUtils.toByteArray(in),
                    java.nio.charset.StandardCharsets.UTF_8);
        }
        assertTrue(!xml.contains("${spring:"), "a ${spring:} lookup cannot resolve without Spring");
        assertTrue(xml.contains("filePattern=\"${sys:LOG_PATH}/dlagent-%d{yyyy-MM-dd}.log\""),
                "the log file must be where the REST agent writes it");
        assertTrue(xml.contains("${sys:dlagent.log.level"), "dlagent.log.level must be honoured");
    }

    @Test
    @DisplayName("gopher properties come from the file, not from the field initialisers")
    void gopherIsBoundFromTheFile() throws Exception {
        GopherConfigurationProperties defaults = new GopherConfigurationProperties();
        assertEquals(Boolean.FALSE, defaults.getEnabled(),
                "if this fails the test has stopped proving anything: the class "
                        + "default must differ from the shipped value");

        GopherConfigurationProperties bound = new GopherConfigurationProperties();
        ConfigurationBinder.bind(bound, GopherConfigurationProperties.PROPERTY_PREFIX, shipped());

        assertEquals(Boolean.TRUE, bound.getEnabled());
        assertEquals(16777216L, bound.getBlockSize());
        assertEquals(16384L, bound.getBufferSize());
        assertEquals("GOPHER_CACHE", bound.getCacheStrategy());
        assertEquals("info", bound.getLogLevel());
    }

    @Test
    @DisplayName("nested dlagent.* properties bind, including final nested objects")
    void nestedPropertiesBind() throws Exception {
        DlServerProperties bound = new DlServerProperties();
        ConfigurationBinder.bind(bound, DlServerProperties.PROPERTY_PREFIX, shipped());

        /* dlagent.tomcat.disable-upload-timeout=false, class default true */
        assertEquals(false, bound.getTomcat().isDisableUploadTimeout());
        /* dlagent.iceberg.hive-catalog-cache-ttl=5m */
        assertEquals(Duration.ofMinutes(5), bound.getIceberg().getHiveCatalogCacheTtl());
        /* dlagent.task.pool.* lives behind a final field with no setter */
        assertEquals(8, bound.getTask().getPool().getCoreSize());
        assertEquals(200, bound.getTask().getPool().getMaxSize());
        assertEquals(0, bound.getTask().getPool().getQueueCapacity());
        assertEquals("dlagent-response-", bound.getTask().getThreadNamePrefix());
    }

    @Test
    @DisplayName("the injector hands out bound properties, not fresh defaults")
    void injectorBindsProperties() throws Exception {
        Injector injector = Guice.createInjector(new DiModule(shipped()));

        assertEquals(Boolean.TRUE,
                injector.getInstance(GopherConfigurationProperties.class).getEnabled());
        assertEquals(Duration.ofMinutes(5),
                injector.getInstance(DlServerProperties.class).getIceberg()
                        .getHiveCatalogCacheTtl());
    }

    @Test
    @DisplayName("an absent key leaves the field initialiser alone")
    void absentKeysKeepDefaults() throws Exception {
        DlConfig empty = DlConfig.load(null, Collections.emptyMap(), new String[0]);
        GopherConfigurationProperties bound = new GopherConfigurationProperties();
        ConfigurationBinder.bind(bound, GopherConfigurationProperties.PROPERTY_PREFIX, empty);

        assertEquals(Boolean.FALSE, bound.getEnabled());
        assertEquals(134217728L, bound.getBlockSize());
    }

    @Test
    @DisplayName("a command line argument still beats the packaged file")
    void commandLineWins() throws Exception {
        DlConfig config;
        try (InputStream in = DlConfig.class.getResourceAsStream("/application.properties")) {
            config = DlConfig.load(in, Collections.emptyMap(),
                                   new String[] {"--gopher.enabled=false"});
        }

        GopherConfigurationProperties bound = new GopherConfigurationProperties();
        ConfigurationBinder.bind(bound, GopherConfigurationProperties.PROPERTY_PREFIX, config);

        /* This is how datalake_proxy turns gopher off in a --without-gopher build. */
        assertEquals(Boolean.FALSE, bound.getEnabled());
    }

    @Test
    @DisplayName("the effective configuration reaches DlEnvironment too")
    void effectiveConfigurationIsCarried() throws Exception {
        Properties p = new Properties();
        p.putAll(shipped().effectiveConfiguration());

        assertTrue(p.containsKey("gopher.enabled"));
        assertEquals("true", p.getProperty("gopher.enabled"));
    }
}
