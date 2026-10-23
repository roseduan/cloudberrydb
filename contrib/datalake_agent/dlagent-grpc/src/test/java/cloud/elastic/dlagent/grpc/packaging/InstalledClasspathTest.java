/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.packaging;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The CVE promise, checked where it is actually made: on what gets INSTALLED.
 *
 * <p>The whole motivation for this migration is that Spring Boot 2.7.18 and
 * spring-framework 5.3.39 are EOL on Java 8/11, so their CVEs have no upstream
 * fix. That benefit is real only if the gRPC agent's runtime classpath carries
 * neither, which is a property of the packaging and not of any pom: a module
 * can be free of Spring and still be launched with Spring on its classpath.
 *
 * <p>So this checks the dependency sets the build produced, and it checks them
 * TWICE over:
 *
 * <ul>
 * <li>by coordinate, for {@code spring-*} and {@code tomcat-embed-*};
 * <li>by CONTENT, for netty. Coordinate matching alone is not enough and
 *     this was measured: {@code grpc-netty-shaded} carries
 *     netty 4.1.132 relocated into {@code io.grpc.netty.shaded.io.netty.*},
 *     so the parent's netty-bom pin (4.1.136, taken for the HTTP/2 Rapid Reset
 *     CVEs) does not reach it and a scan over Maven coordinates reports one
 *     component and a clean bill of health while a vulnerable HTTP/2 codec
 *     ships anyway. This test fails on any jar that contains relocated netty
 *     classes, which is why the build uses the non-shaded grpc-netty.
 * </ul>
 */
public class InstalledClasspathTest {

    private static Path agentRoot() {
        return Paths.get("..").toAbsolutePath().normalize();
    }

    /**
     * The module's dependencies, as files that exist.
     *
     * <p>Read from {@code target/classpath-src.txt} -- maven's own list, the
     * same one {@code install-layout.sh} copies from, so this test and the
     * installer can never disagree about what ships. There is deliberately
     * no {@code target/deps} copy of every jar: four modules inheriting a
     * copy-dependencies execution write 4.1 GB between them.
     *
     * <p>A missing or empty list FAILS rather than returning nothing. Every
     * assertion here is of the form "no jar in this set is bad", so an empty
     * set passes all of them: the old {@code return new ArrayList<>()} on a
     * missing directory turned the entire CVE check into a no-op on exactly
     * the builds where something had gone wrong with packaging.
     */
    private static List<Path> depsOf(String module) throws IOException {
        Path listing = agentRoot().resolve("dlagent-" + module)
                                  .resolve("target").resolve("classpath-src.txt");
        assertTrue(Files.isRegularFile(listing),
                "missing " + listing + "; run mvn package first. This test cannot check a "
                        + "classpath it cannot see, and must not pass by default.");
        String text = new String(Files.readAllBytes(listing), StandardCharsets.UTF_8).trim();
        assertTrue(!text.isEmpty(), listing + " is empty; see the comment on depsOf");

        List<Path> jars = new ArrayList<>();
        for (String entry : text.split(":")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            Path jar = Paths.get(trimmed);
            /*
             * A sibling module resolves to target/classes rather than to a
             * jar when the session that wrote this list did not package it --
             * `mvn test` does not, and this test runs under exactly that.
             * Those are our own modules; the question here is what THIRD
             * PARTY code ships, so they are skipped rather than chased to
             * their jar. Anything else that is not a file is a real problem.
             */
            if (Files.isDirectory(jar)) {
                assertTrue(jar.endsWith("classes") && jar.toString().contains("/dlagent-"),
                        listing + " lists a directory that is not a sibling module: " + jar);
                continue;
            }
            assertTrue(Files.isRegularFile(jar),
                    listing + " lists a file that does not exist: " + jar);
            /*
             * Not everything on a classpath is a zip: apache-curator ships a
             * .pom there. It carries no classes, so it has nothing to say
             * about Spring, Tomcat or netty, and noJarCarriesRelocatedNetty
             * opens every one of these with JarFile.
             */
            if (!jar.getFileName().toString().endsWith(".jar")) {
                continue;
            }
            jars.add(jar);
        }
        jars.sort(null);
        return jars;
    }

    @Test
    public void theGrpcClasspathCarriesNoSpringOrTomcat() throws IOException {
        List<Path> deps = depsOf("grpc");
        assertTrue(deps.size() > 5,
                "expected the grpc module's collected dependencies; run mvn package first. "
                        + "Found " + deps.size() + " under " + agentRoot());

        List<String> offenders = new ArrayList<>();
        for (Path jar : deps) {
            String name = jar.getFileName().toString().toLowerCase(Locale.ROOT);
            if (name.startsWith("spring-") || name.startsWith("tomcat-embed")) {
                offenders.add(name);
            }
        }
        assertEquals(new ArrayList<String>(), offenders,
                "the gRPC agent would ship with Spring or Tomcat on its classpath, which is "
                        + "the entire CVE benefit of this migration");
    }

    /**
     * core/ is shared, so anything Spring-shaped there reaches the gRPC agent
     * too. This is the half a per-module SBOM misses: dlagent-core's pom is
     * Spring-free, and that says nothing about what is installed next to it.
     */
    @Test
    public void theSharedCoreCarriesNoSpringOrTomcat() throws IOException {
        List<String> offenders = new ArrayList<>();
        for (Path jar : depsOf("core")) {
            String name = jar.getFileName().toString().toLowerCase(Locale.ROOT);
            if (name.startsWith("spring-") || name.startsWith("tomcat-embed")) {
                offenders.add(name);
            }
        }
        assertEquals(new ArrayList<String>(), offenders,
                "core is on both transports' classpath, so Spring here reaches the gRPC agent");
    }

    /**
     * Relocated netty that predates this work, with the reason each is
     * tolerated. They are NOT waved through as a formality: each is a real
     * copy of netty that the parent's netty-bom pin cannot reach and a
     * coordinate scan cannot see, i.e. exactly the hazard this test is about,
     * arriving from a different vendor. Removing them means dropping
     * hadoop-aws and hbase, which is a separate decision from this migration.
     *
     * <p>Listing them by name is what keeps the test useful: anything NEW
     * still fails, including grpc-netty-shaded, which is the one this
     * migration could have introduced and deliberately did not.
     */
    private static final TreeSet<String> KNOWN_RELOCATED_NETTY = new TreeSet<>(java.util.Arrays.asList(
            // AWS SDK v2's uber jar; io.netty relocated under
            // software.amazon.awssdk.thirdparty. Arrives via hadoop-aws 3.4.3
            // and is 654 MB of the installed size.
            "bundle-2.35.4.jar",
            // HBase's own shaded netty, via hbase-client/hbase-server under
            // hive-metastore and hudi-common.
            "hbase-shaded-netty-3.5.1.jar"));

    @Test
    public void noJarCarriesRelocatedNetty() throws IOException {
        List<String> offenders = new ArrayList<>();
        List<Path> all = new ArrayList<>();
        all.addAll(depsOf("core"));
        all.addAll(depsOf("grpc"));

        for (Path jar : all) {
            if (KNOWN_RELOCATED_NETTY.contains(jar.getFileName().toString())) {
                continue;
            }
            try (JarFile file = new JarFile(new File(jar.toString()))) {
                Enumeration<JarEntry> entries = file.entries();
                while (entries.hasMoreElements()) {
                    String entry = entries.nextElement().getName();
                    // The relocated form: any package path that ends in
                    // io/netty/ but does not start there.
                    if (entry.contains("/io/netty/") && !entry.startsWith("io/netty/")) {
                        offenders.add(jar.getFileName() + " -> " + entry);
                        break;
                    }
                }
            }
        }
        assertEquals(new ArrayList<String>(), offenders,
                "a jar carries netty classes under a relocated package, where the parent's "
                        + "netty-bom pin cannot reach them and a coordinate scan cannot see "
                        + "them. Use the non-shaded artifact, or add it to "
                        + "KNOWN_RELOCATED_NETTY with the reason it cannot be removed.");
    }

    /**
     * The known exceptions must stay known: if one disappears from the build,
     * the entry should go too, or the list slowly becomes a place where real
     * problems hide.
     */
    @Test
    public void theKnownRelocatedNettyJarsAreStillPresent() throws IOException {
        TreeSet<String> present = new TreeSet<>();
        for (Path p : depsOf("core")) {
            present.add(p.getFileName().toString());
        }
        TreeSet<String> stale = new TreeSet<>(KNOWN_RELOCATED_NETTY);
        stale.removeAll(present);
        assertEquals(new TreeSet<String>(), stale,
                "these are exempted from the relocated-netty check but are no longer on the "
                        + "classpath; drop them from KNOWN_RELOCATED_NETTY");
    }

    /**
     * The shared dependencies must be stored once. Duplicating them would put
     * the installed size at roughly twice the pre-migration jar instead of
     * just over it, and would also mean two
     * copies of a library that a CVE fix then has to reach twice.
     */
    @Test
    public void transportsShareRatherThanDuplicateTheirDependencies() throws IOException {
        TreeSet<String> core = new TreeSet<>();
        for (Path p : depsOf("core")) {
            core.add(p.getFileName().toString());
        }
        TreeSet<String> grpcOnly = new TreeSet<>();
        for (Path p : depsOf("grpc")) {
            String name = p.getFileName().toString();
            if (!core.contains(name) && !name.startsWith("dlagent-core")) {
                grpcOnly.add(name);
            }
        }
        assertTrue(core.size() > 100,
                "expected core to hold the bulk of the dependencies, saw " + core.size());
        assertTrue(grpcOnly.size() < 40,
                "the gRPC transport should add only the gRPC stack on top of core, but adds "
                        + grpcOnly.size() + ": " + grpcOnly);
    }

    /** The contract file the C side reads must be generated, and ordered. */
    @Test
    public void theOrderedClasspathFileIsGenerated() throws IOException {
        Path cp = agentRoot().resolve("dlagent-grpc").resolve("target").resolve("classpath.txt");
        assertTrue(Files.exists(cp), "missing " + cp + "; the install layout depends on it");
        String text = new String(Files.readAllBytes(cp), StandardCharsets.UTF_8);
        assertTrue(text.contains("grpc-netty"), "the gRPC stack is missing from " + cp);
        assertTrue(text.split(":").length > 100,
                "the classpath looks truncated: " + text.split(":").length + " entries");
    }
}
