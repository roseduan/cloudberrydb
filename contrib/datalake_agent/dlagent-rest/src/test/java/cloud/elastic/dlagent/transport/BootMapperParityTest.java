/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the single serialization exit: {@code DlObjectMapperFactory.create()}
 * in dlagent-core must be configured exactly like Spring Boot's
 * auto-configured ObjectMapper, because the REST path used to serialize its
 * responses with the latter and the gRPC path has to produce the same bytes.
 *
 * <p>This test lives in dlagent-rest because it is the only module that can see
 * Boot at all -- core may not import Spring. It is the cross-check
 * that keeps the reproduction in {@code DlObjectMapperFactory} honest when
 * spring-boot or jackson is upgraded: a shifted default fails the build here
 * instead of silently splitting the two transports on dates, nulls or field
 * order, which no semantic test would notice.
 */
public class BootMapperParityTest {

    /**
     * The only differences allowed between core's mapper and Boot's. Both are
     * Boot-internal modules that contribute nothing unless the application
     * declares the matching annotation, and {@link #noBootOnlyJacksonAnnotationsExist()}
     * proves this repository declares neither.
     */
    private static final Set<String> BOOT_ONLY_MODULES = new TreeSet<>(Arrays.asList(
            "org.springframework.boot.jackson.JsonComponentModule",
            "org.springframework.boot.jackson.JsonMixinModule"));

    private static Map<String, String> config(ObjectMapper m) {
        Map<String, String> out = new LinkedHashMap<>();
        for (SerializationFeature f : SerializationFeature.values()) {
            out.put("SER." + f.name(), String.valueOf(m.isEnabled(f)));
        }
        for (DeserializationFeature f : DeserializationFeature.values()) {
            out.put("DES." + f.name(), String.valueOf(m.isEnabled(f)));
        }
        for (MapperFeature f : MapperFeature.values()) {
            out.put("MAP." + f.name(), String.valueOf(m.isEnabled(f)));
        }
        JsonInclude.Value inc = m.getSerializationConfig().getDefaultPropertyInclusion();
        out.put("INCLUSION.value", String.valueOf(inc == null ? null : inc.getValueInclusion()));
        out.put("INCLUSION.content", String.valueOf(inc == null ? null : inc.getContentInclusion()));
        out.put("TIMEZONE", m.getSerializationConfig().getTimeZone().getID());
        out.put("LOCALE", String.valueOf(m.getSerializationConfig().getLocale()));
        return out;
    }

    private static Set<String> moduleIds(ObjectMapper m) {
        return m.getRegisteredModuleIds().stream()
                .map(String::valueOf)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Test
    public void coreMapperMatchesBootAutoConfiguredMapper() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
                .run(ctx -> {
                    ObjectMapper boot = ctx.getBean(ObjectMapper.class);
                    ObjectMapper core = DlObjectMapperFactory.create();

                    Map<String, String> bootCfg = config(boot);
                    Map<String, String> coreCfg = config(core);

                    List<String> deltas = new ArrayList<>();
                    for (String key : bootCfg.keySet()) {
                        String b = bootCfg.get(key);
                        String c = coreCfg.get(key);
                        if (b == null ? c != null : !b.equals(c)) {
                            deltas.add(key + ": boot=" + b + " core=" + c);
                        }
                    }
                    assertEquals(java.util.Collections.emptyList(), deltas,
                            "core's ObjectMapper drifted from Boot's auto-configured one. "
                                    + "Every entry below is a way the two transports could emit "
                                    + "different bytes for the same response. Fix "
                                    + "DlObjectMapperFactory to match, do not relax this test.");

                    Set<String> onlyInBoot = new TreeSet<>(moduleIds(boot));
                    onlyInBoot.removeAll(moduleIds(core));
                    assertEquals(BOOT_ONLY_MODULES, onlyInBoot,
                            "the set of Jackson modules Boot registers and core does not has "
                                    + "changed. Only the two Boot-internal no-op modules may be "
                                    + "missing from core; anything else is a real serialization "
                                    + "difference.");

                    Set<String> onlyInCore = new TreeSet<>(moduleIds(core));
                    onlyInCore.removeAll(moduleIds(boot));
                    assertEquals(java.util.Collections.emptySet(), onlyInCore,
                            "core registers a Jackson module Boot does not, so core would emit "
                                    + "bytes the REST path never emitted.");
                });
    }

    /**
     * The reason omitting JsonComponentModule and JsonMixinModule from core is
     * safe rather than merely convenient: they only ever contribute serializers
     * or mix-ins that the application declares with {@code @JsonComponent} /
     * {@code @JsonMixin}, and this repository declares none. If someone adds
     * one, core's mapper would stop seeing it while REST's kept seeing it --
     * the two transports would diverge on exactly that type -- so this fails
     * the build and forces the question to be answered explicitly.
     */
    @Test
    public void noBootOnlyJacksonAnnotationsExist() throws IOException {
        Path agentRoot = Paths.get("..").toAbsolutePath().normalize();
        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        try (Stream<Path> files = Files.walk(agentRoot)) {
            List<Path> javaFiles = files
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !p.toString().contains("/target/"))
                    .collect(Collectors.toList());
            for (Path file : javaFiles) {
                scanned++;
                String text = new String(Files.readAllBytes(file), java.nio.charset.StandardCharsets.UTF_8);
                if (usesBootJacksonAnnotation(text)) {
                    offenders.add(agentRoot.relativize(file).toString());
                }
            }
        }
        // A scan that walked nothing would pass forever.
        assertTrue(scanned > 100, "expected to scan the whole agent tree, only saw " + scanned
                + " java files under " + agentRoot);
        assertEquals(java.util.Collections.emptyList(), offenders,
                "@JsonComponent / @JsonMixin appeared. Boot's JsonComponentModule and "
                        + "JsonMixinModule are no longer no-ops, so DlObjectMapperFactory's "
                        + "reproduction of Boot's mapper is now incomplete and the two "
                        + "transports will serialize this type differently.");
    }

    /**
     * True when the file actually uses one of the two annotations, rather than
     * merely naming it.
     *
     * <p>Detecting the bare token would flag every javadoc paragraph that
     * explains why these two modules are safe to omit -- including
     * DlObjectMapperFactory's, which exists precisely to explain that. So the
     * signal is use: an annotation at the start of a line, or an import of the
     * annotation type, which is unavoidable for any real usage.
     */
    private static boolean usesBootJacksonAnnotation(String text) {
        for (String line : text.split("\n", -1)) {
            String t = line.trim();
            if (t.startsWith("@JsonComponent") || t.startsWith("@JsonMixin")) {
                return true;
            }
            if (t.startsWith("import org.springframework.boot.jackson.JsonComponent")
                    || t.startsWith("import org.springframework.boot.jackson.JsonMixin")) {
                return true;
            }
        }
        return false;
    }
}
