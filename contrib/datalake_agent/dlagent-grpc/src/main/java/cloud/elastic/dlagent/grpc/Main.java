/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.config.DlConfig;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.logging.log4j.jul.Log4jBridgeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * The gRPC agent's entry point: the composition root, and nothing else.
 *
 * <p>Its counterpart is {@code ServiceApplication} in dlagent-rest. Neither
 * contains business logic; they differ only in which container assembles the
 * same core objects.
 */
public final class Main {

    /** Where the packaged defaults live, same file the REST jar ships. */
    private static final String PROPERTIES = "/application.properties";

    /** What REST's logging.config names, when the configuration names nothing. */
    private static final String DEFAULT_LOGGING_CONFIG = "classpath:log4j2-dlagent.xml";

    public static void main(String[] args) {
        DlConfig config;
        try (InputStream in = Main.class.getResourceAsStream(PROPERTIES)) {
            config = DlConfig.load(in, System.getenv(), args);
        } catch (Exception e) {
            // Before logging exists; stderr reaches the server log.
            System.err.println("dlagent gRPC failed to read its configuration: " + e);
            System.exit(1);
            return;
        }
        // Before the first logger: log4j reads its configuration once, when
        // the first one is created.
        configureLogging(config, System.getProperties());
        Logger log = LoggerFactory.getLogger(Main.class);

        DlAgentServer server = null;
        try {
            Injector injector = compose(config);
            HealthService health = injector.getInstance(HealthService.class);
            IcebergGrpcServices services = injector.getInstance(IcebergGrpcServices.class);
            DataProxyGrpcService dataProxy = injector.getInstance(DataProxyGrpcService.class);

            server = new DlAgentServer(config, health, services, dataProxy);
            // Phase one: the port is what datalake_proxy watches for.
            server.listen();
            // Phase two: only now may a request be served.
            server.becomeReady();

            final DlAgentServer running = server;
            Runtime.getRuntime().addShutdownHook(new Thread(running::shutdown, "dlagent-shutdown"));
            server.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Exit non-zero rather than lingering as a listening socket that
            // can never answer: datalake_proxy can restart a dead child, but
            // it has no way to tell that a live one is permanently broken.
            log.error("dlagent gRPC failed to start", e);
            if (server != null) {
                server.shutdown();
            }
            System.exit(1);
        }
    }

    /**
     * Sets up logging the way spring-boot does for the REST agent, so both
     * write the same dlagent-*.log in the same place and format.
     *
     * <p>spring-boot's LoggingSystemProperties turns these logging.* keys into
     * the system properties log4j2-dlagent.xml reads ({@code LOG_PATH} from
     * logging.file.path, the charsets defaulting to UTF-8, {@code PID}, ...),
     * and its {@code ${spring:dlagent.log.level}} lookup reads the level from
     * the environment. Without Spring both are done here, from the same
     * configuration; the gRPC copy of the XML reads the level from
     * {@code dlagent.log.level}. A system property already set on the command
     * line is left alone.
     *
     * @param config     the resolved configuration
     * @param properties where to set them, the JVM's system properties
     */
    static void configureLogging(DlConfig config, Properties properties) {
        setIfAbsent(properties, "LOG_PATH", config.get("logging.file.path"));
        setIfAbsent(properties, "LOG_FILE", config.get("logging.file.name"));
        setIfAbsent(properties, "FILE_LOG_CHARSET",
                config.get("logging.charset.file", StandardCharsets.UTF_8.name()));
        setIfAbsent(properties, "CONSOLE_LOG_CHARSET",
                config.get("logging.charset.console", StandardCharsets.UTF_8.name()));
        setIfAbsent(properties, "LOG_EXCEPTION_CONVERSION_WORD",
                config.get("logging.exception-conversion-word"));
        setIfAbsent(properties, "FILE_LOG_PATTERN", config.get("logging.pattern.file"));
        setIfAbsent(properties, "LOG_LEVEL_PATTERN", config.get("logging.pattern.level"));
        setIfAbsent(properties, "LOG_DATEFORMAT_PATTERN",
                config.get("logging.pattern.dateformat"));
        String runtime = ManagementFactory.getRuntimeMXBean().getName();
        setIfAbsent(properties, "PID", runtime.substring(0, Math.max(0, runtime.indexOf('@'))));
        setIfAbsent(properties, "dlagent.log.level", config.get("dlagent.log.level"));

        String location = config.get("logging.config", DEFAULT_LOGGING_CONFIG);
        if (location.startsWith("classpath:")) {
            URL resource = Main.class.getResource(
                    "/" + location.substring("classpath:".length()).replaceFirst("^/", ""));
            location = resource == null ? null : resource.toString();
        }
        setIfAbsent(properties, "log4j2.configurationFile", location);

        installJulBridge();
    }

    /**
     * Routes java.util.logging, which grpc-java and Netty's grpc transport
     * log through, into log4j; otherwise their warnings go to JUL's console
     * handler on stderr and never reach dlagent.log. Under REST spring-boot
     * installs the equivalent bridge.
     *
     * <p>A bridge handler rather than the java.util.logging.manager system
     * property: the property only works if set before anything touches JUL,
     * which a library on the classpath can defeat, while the handler can be
     * installed at any time. propagateLevels copies log4j's levels onto the
     * JUL loggers, so records log4j would drop are not even created.
     */
    static void installJulBridge() {
        Log4jBridgeHandler.install(true, null, true);
    }

    private static void setIfAbsent(Properties properties, String key, String value) {
        if (value != null && !value.isEmpty() && properties.getProperty(key) == null) {
            properties.setProperty(key, value);
        }
    }

    /**
     * Builds the object graph the agent serves from, and installs it behind
     * {@code BeanLocator} before anything can be served: the data plane
     * creates its metadata fetchers reflectively, and their no-arg
     * constructors resolve their collaborators there.  Under REST the Spring
     * context does the same through {@code SpringBeanLocatorInstaller}.
     *
     * @param config the resolved configuration
     * @return the injector
     */
    static Injector compose(DlConfig config) {
        Injector injector = Guice.createInjector(new DiModule(config));
        DiModule.installBeanLocator(injector);
        return injector;
    }

    private Main() {
    }
}
