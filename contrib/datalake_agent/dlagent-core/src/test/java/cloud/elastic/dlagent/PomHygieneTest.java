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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A child module must never re-declare a version for a dependency the parent
 * already manages.
 * <p>
 * The reason this rule exists is concrete: the abandoned gRPC branch pinned
 * jackson to 2.16.1 in a child pom while the parent was already on 2.21.5,
 * quietly undoing five CVE hardening commits. One place to bump, one place to
 * audit.
 * <p>
 * It is a test rather than an enforcer rule because maven-enforcer 3.4.1 has
 * no built-in rule for it: {@code banDuplicatePomDependencyVersions} (which
 * the parent does run) only catches a coordinate declared twice within one
 * pom, and there is no rule that compares a child's declarations against the
 * inherited dependencyManagement.
 */
class PomHygieneTest {

    private static final File PARENT_POM = new File("../pom.xml");

    private static final List<File> MODULE_POMS = Arrays.asList(
            new File("../dlagent-core/pom.xml"),
            new File("../dlagent-rest/pom.xml"),
            new File("../dlagent-grpc/pom.xml"),
            // Tests only, ships nothing -- but a version pinned there would
            // drift from the parent just as easily as anywhere else.
            new File("../dlagent-parity/pom.xml"));

    @Test
    void modulePomsDoNotRepeatManagedVersions() throws Exception {
        Set<String> managed = managedCoordinates(PARENT_POM);
        assertTrue(managed.size() > 20,
                "expected the parent to manage a large dependency set, found " + managed.size());

        List<String> violations = new ArrayList<>();
        for (File pom : MODULE_POMS) {
            assertTrue(pom.isFile(), "missing module pom: " + pom.getPath());
            Document document = parse(pom);
            for (Element dependency : directDependencies(document)) {
                String coordinate = text(dependency, "groupId") + ":" + text(dependency, "artifactId");
                String version = text(dependency, "version");
                if (!version.isEmpty() && managed.contains(coordinate)) {
                    violations.add(pom.getPath() + " declares <version>" + version + "</version> for "
                            + coordinate + ", which the parent already manages");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    /**
     * Among the modules that SHIP, only dlagent-rest may declare spring.
     *
     * <p>dlagent-parity is exempt and the exemption is safe rather than
     * convenient: it produces no installed artifact (install-layout.sh lays out
     * core/, rest/ and grpc/ by name, and its own pom skips install and
     * deploy), and its spring dependency is test-scoped MockMvc, which exists
     * so that one JVM can drive both transports and compare their bytes. The
     * invariant this test protects -- no spring on the gRPC agent's runtime
     * classpath -- is asserted directly, against the installed layout, by
     * InstalledClasspathTest.
     */
    @Test
    void onlyDlagentRestDependsOnSpring() throws Exception {
        for (File pom : MODULE_POMS) {
            if (pom.getPath().contains("dlagent-parity")) {
                continue;
            }
            Document document = parse(pom);
            boolean restModule = pom.getPath().contains("dlagent-rest");
            for (Element dependency : directDependencies(document)) {
                String groupId = text(dependency, "groupId");
                boolean springOrTomcat = groupId.startsWith("org.springframework")
                        || groupId.startsWith("org.apache.tomcat");
                if (springOrTomcat && !restModule) {
                    assertFalse(springOrTomcat,
                            pom.getPath() + " must not depend on " + groupId
                                    + ":" + text(dependency, "artifactId"));
                }
            }
        }
    }

    private static Set<String> managedCoordinates(File pom) throws Exception {
        Document document = parse(pom);
        Set<String> coordinates = new HashSet<>();
        NodeList managementNodes = document.getDocumentElement()
                .getElementsByTagName("dependencyManagement");
        for (int i = 0; i < managementNodes.getLength(); i++) {
            Element management = (Element) managementNodes.item(i);
            for (Element dependency : childDependencies(management)) {
                coordinates.add(text(dependency, "groupId") + ":" + text(dependency, "artifactId"));
            }
        }
        return coordinates;
    }

    /** The {@code <dependencies>} directly under {@code <project>}. */
    private static List<Element> directDependencies(Document document) {
        List<Element> result = new ArrayList<>();
        for (Node node = document.getDocumentElement().getFirstChild();
                node != null; node = node.getNextSibling()) {
            if (node.getNodeType() == Node.ELEMENT_NODE && "dependencies".equals(node.getNodeName())) {
                result.addAll(dependencyElements((Element) node));
            }
        }
        return result;
    }

    private static List<Element> childDependencies(Element parent) {
        List<Element> result = new ArrayList<>();
        NodeList lists = parent.getElementsByTagName("dependencies");
        for (int i = 0; i < lists.getLength(); i++) {
            result.addAll(dependencyElements((Element) lists.item(i)));
        }
        return result;
    }

    private static List<Element> dependencyElements(Element dependencies) {
        List<Element> result = new ArrayList<>();
        for (Node node = dependencies.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node.getNodeType() == Node.ELEMENT_NODE && "dependency".equals(node.getNodeName())) {
                result.add((Element) node);
            }
        }
        return result;
    }

    /** Direct child element text, empty when the element is absent. */
    private static String text(Element parent, String name) {
        for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node.getNodeType() == Node.ELEMENT_NODE && name.equals(node.getNodeName())) {
                return node.getTextContent().trim();
            }
        }
        return "";
    }

    private static Document parse(File pom) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(pom);
    }
}
