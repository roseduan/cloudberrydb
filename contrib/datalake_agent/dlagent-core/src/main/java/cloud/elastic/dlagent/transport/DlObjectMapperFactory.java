/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * Builds the ONE ObjectMapper the agent uses to serialize responses, for both
 * transports (the "single serialization exit").
 *
 * <p>WHY THIS CLASS EXISTS, AND WHY THE FLAGS BELOW ARE NOT ARBITRARY.
 *
 * <p>Before the transport split the REST controller returned POJOs and let Spring's
 * MappingJackson2HttpMessageConverter serialize them with Spring Boot's
 * <em>auto-configured</em> ObjectMapper. That mapper is NOT a bare
 * {@code new ObjectMapper()}. The gRPC transport must hand the C side the same
 * bytes, and dlagent-core may not import Spring, so the Boot mapper has to be reproduced here rather
 * than borrowed -- and reproduced from measurement, not from documentation.
 *
 * <p>MEASURED on 2026-09-11 against spring-boot 2.7.18 + jackson 2.21.5 in this
 * repository, by dumping every SerializationFeature / DeserializationFeature /
 * MapperFeature of Boot's auto-configured bean and diffing it against a bare
 * mapper. The complete delta was four flags and five modules:
 *
 * <pre>
 *   SER.WRITE_DATES_AS_TIMESTAMPS       true  -> false
 *   SER.WRITE_DURATIONS_AS_TIMESTAMPS   true  -> false
 *   DES.FAIL_ON_UNKNOWN_PROPERTIES      true  -> false
 *   MAP.DEFAULT_VIEW_INCLUSION          true  -> false
 *   modules  []  ->  [Jdk8Module, jackson-datatype-jsr310,
 *                     jackson-module-parameter-names,
 *                     org.springframework.boot.jackson.JsonComponentModule,
 *                     org.springframework.boot.jackson.JsonMixinModule]
 * </pre>
 *
 * <p>The two Boot-only modules are deliberately NOT reproduced, and that is
 * safe rather than convenient: JsonComponentModule only contributes serializers
 * discovered via {@code @JsonComponent} and JsonMixinModule only mix-ins
 * discovered via {@code @JsonMixin}, and this repository contains zero of each
 * (verified by scanning the whole tree). They are no-ops here, so omitting them
 * changes no byte. {@code BootMapperParityTest} in dlagent-rest keeps both
 * halves of that claim honest: it re-runs the config diff against the live Boot
 * bean, and it re-scans for the two annotations.
 *
 * <p>Everything else was already identical, including the two that matter most
 * for output shape and are therefore asserted rather than set: the default
 * property inclusion is USE_DEFAULTS (nulls ARE written -- note this is NOT the
 * NON_EMPTY inclusion that {@code BaseBridge} sets on its own private mapper
 * for bridge configs) and the timezone is UTC.
 */
public final class DlObjectMapperFactory {

    private DlObjectMapperFactory() {
    }

    /**
     * A new mapper configured exactly like Spring Boot's auto-configured one,
     * minus the two Boot-only modules proven to be no-ops here.
     *
     * <p>Callers should not need this: inject {@link JsonResponseWriter} and use
     * the single instance it owns. It is public only so each composition root
     * (Spring in dlagent-rest, Guice in dlagent-grpc) can build that one
     * instance, and so the parity test can diff it.
     */
    public static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(MapperFeature.DEFAULT_VIEW_INCLUSION);
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new ParameterNamesModule());
        return mapper;
    }
}
