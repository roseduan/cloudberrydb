/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.config.DlConfig;
import org.junit.jupiter.api.Test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Main hands log4j what spring-boot hands it under REST, from the same
 * configuration, so the gRPC agent writes the same dlagent-*.log.
 */
public class MainLoggingTest {

    private static Properties configure(String... args) throws Exception {
        DlConfig config;
        try (InputStream in = Main.class.getResourceAsStream("/application.properties")) {
            config = DlConfig.load(in,
                    Collections.singletonMap("COORDINATOR_DATA_DIRECTORY", "/data/qd"), args);
        }
        Properties properties = new Properties();
        Main.configureLogging(config, properties);
        return properties;
    }

    @Test
    public void setsWhatSpringBootSets() throws Exception {
        Properties p = configure();
        assertEquals("/data/qd/log", p.getProperty("LOG_PATH"));
        assertEquals("/data/qd/log/dlagent.log", p.getProperty("LOG_FILE"));
        assertEquals("UTF-8", p.getProperty("FILE_LOG_CHARSET"));
        assertEquals("info", p.getProperty("dlagent.log.level"));
        assertTrue(p.getProperty("PID").matches("\\d+"), p.getProperty("PID"));
        assertTrue(p.getProperty("log4j2.configurationFile").endsWith("/log4j2-dlagent.xml"),
                p.getProperty("log4j2.configurationFile"));
    }

    @Test
    public void theLevelFollowsTheCommandLine() throws Exception {
        assertEquals("debug", configure("--dlagent.log.level=debug")
                .getProperty("dlagent.log.level"));
    }

    @Test
    public void anExplicitSystemPropertyWins() throws Exception {
        DlConfig config;
        try (InputStream in = Main.class.getResourceAsStream("/application.properties")) {
            config = DlConfig.load(in, Collections.emptyMap(), new String[0]);
        }
        Properties properties = new Properties();
        properties.setProperty("LOG_PATH", "/elsewhere");
        Main.configureLogging(config, properties);
        assertEquals("/elsewhere", properties.getProperty("LOG_PATH"));
    }

    /** Collects what reaches log4j. */
    private static final class Capture extends AbstractAppender {
        final List<LogEvent> events = new CopyOnWriteArrayList<>();

        Capture() {
            super("jul-bridge-capture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }

    /**
     * grpc-java logs through java.util.logging. Those records must reach
     * log4j, and so dlagent.log, rather than JUL's console handler on stderr.
     */
    @Test
    public void javaUtilLoggingReachesLog4j() throws Exception {
        configure();

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        Capture capture = new Capture();
        capture.start();
        configuration.addAppender(capture);
        configuration.getRootLogger().addAppender(capture, Level.ALL, null);
        Configurator.setLevel("io.grpc.test", Level.ALL);
        try {
            java.util.logging.Logger.getLogger("io.grpc.test")
                    .warning("a grpc warning that belongs in dlagent.log");

            assertTrue(capture.events.stream().anyMatch(e ->
                            "io.grpc.test".equals(e.getLoggerName())
                                    && e.getLevel() == Level.WARN
                                    && e.getMessage().getFormattedMessage()
                                            .equals("a grpc warning that belongs in dlagent.log")),
                    "the JUL record never reached log4j: " + capture.events);
            for (java.util.logging.Handler h :
                    java.util.logging.Logger.getLogger("").getHandlers()) {
                assertTrue(!(h instanceof java.util.logging.ConsoleHandler),
                        "JUL still writes to the console: " + h);
            }
        } finally {
            configuration.getRootLogger().removeAppender(capture.getName());
            capture.stop();
        }
    }
}
