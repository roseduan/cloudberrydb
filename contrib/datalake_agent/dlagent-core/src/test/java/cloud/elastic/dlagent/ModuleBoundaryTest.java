/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * dlagent-core must not reach for a transport.
 * <p>
 * No {@code org.springframework.*}, no
 * {@code javax.servlet.*} and no {@code org.apache.catalina.*} in core's
 * sources. The catalina clause is not decoration -- core carried four
 * references to {@code ClientAbortException} before this migration, and
 * letting core depend on tomcat-embed-core to keep them compiling would put
 * Tomcat on the gRPC runtime classpath, which must carry no Spring and no
 * Tomcat at all.
 * <p>
 * Also forbidden: importing back into the transport modules. That is what
 * turns the module graph into a cycle, which is why {@code FileListRequest}
 * and {@code CatalogOperationException} had to move into core packages first.
 * <p>
 * A forbidden <em>import</em> mostly cannot compile today, since none of
 * those artifacts are on core's compile classpath - which is the point of
 * this test. It does not guard against the compile error; it guards against
 * the tempting fix for it, adding {@code tomcat-embed-core} (or
 * spring-boot-starter-web, or a servlet api) to dlagent-core so the import
 * resolves. That builds cleanly and puts Tomcat on the gRPC runtime
 * classpath. It also catches inline fully-qualified references, which is how
 * the two reverse dependencies were written.
 * <p>
 * Scans {@code src/main/java}, not {@code .../cloud/elastic/dlagent}: three
 * business classes deliberately live under {@code org.apache.*} to reach
 * package-private Hadoop and Iceberg internals, and the narrower scan root
 * misses them.
 */
class ModuleBoundaryTest {

    private static final Path SOURCE_ROOT = Paths.get("src", "main", "java");

    private static final List<String> FORBIDDEN_IMPORT_PREFIXES = Arrays.asList(
            "import org.springframework.",
            "import javax.servlet.",
            "import org.apache.catalina.",
            "import cloud.elastic.dlagent.service.rest.",
            "import cloud.elastic.dlagent.service.spring.");

    /**
     * Fully qualified names used inline (without an import) are just as much a
     * dependency, and this codebase does use that style - the old
     * {@code RequestContext.fileList} field named
     * {@code cloud.elastic.dlagent.service.rest.FileListRequest.FileEntry} in
     * place.
     */
    private static final List<String> FORBIDDEN_INLINE_REFERENCES = Arrays.asList(
            "org.springframework.",
            "javax.servlet.",
            "org.apache.catalina.",
            "cloud.elastic.dlagent.service.rest.",
            "cloud.elastic.dlagent.service.spring.");

    @Test
    void coreSourcesReferenceNoTransportPackage() throws IOException {
        List<String> violations = new ArrayList<>();
        int scanned = 0;

        try (Stream<Path> files = Files.walk(SOURCE_ROOT)) {
            List<Path> javaFiles = new ArrayList<>();
            files.filter(p -> p.toString().endsWith(".java")).forEach(javaFiles::add);
            for (Path file : javaFiles) {
                scanned++;
                int lineNumber = 0;
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    lineNumber++;
                    String code = stripComment(line);
                    for (String forbidden : FORBIDDEN_IMPORT_PREFIXES) {
                        if (code.trim().startsWith(forbidden)) {
                            violations.add(file + ":" + lineNumber + " " + code.trim());
                        }
                    }
                    for (String forbidden : FORBIDDEN_INLINE_REFERENCES) {
                        if (!code.trim().startsWith("import ") && code.contains(forbidden)) {
                            violations.add(file + ":" + lineNumber + " " + code.trim());
                        }
                    }
                }
            }
        }

        /*
         * A scan that silently walks nothing would pass forever; core has ~160
         * main sources, so anything near zero means the root moved.
         */
        assertTrue(scanned > 100,
                "expected to scan the whole core source tree, only saw " + scanned + " files");
        assertTrue(violations.isEmpty(),
                "dlagent-core must not reference a transport package:\n"
                        + String.join("\n", violations));
    }

    @Test
    void scanRootExists() {
        assertTrue(Files.isDirectory(SOURCE_ROOT),
                "scan root " + SOURCE_ROOT.toAbsolutePath() + " does not exist");
        assertFalse(Files.isDirectory(SOURCE_ROOT.resolve("cloud/elastic/dlagent/service/rest")),
                "service/rest belongs to dlagent-rest");
        assertFalse(Files.isDirectory(SOURCE_ROOT.resolve("cloud/elastic/dlagent/service/spring")),
                "service/spring belongs to dlagent-rest");
    }

    /**
     * Drops {@code //} line comments and the body of a javadoc/block comment
     * line so that prose naming a forbidden package (several comments do, on
     * purpose) is not read as a dependency.
     */
    private static String stripComment(String line) {
        String result = line;
        int slashes = result.indexOf("//");
        if (slashes >= 0) {
            result = result.substring(0, slashes);
        }
        String trimmed = result.trim();
        if (trimmed.startsWith("*") || trimmed.startsWith("/*")) {
            return "";
        }
        return result;
    }
}
