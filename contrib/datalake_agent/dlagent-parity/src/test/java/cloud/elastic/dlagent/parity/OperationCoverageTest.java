/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The third side of the operation contract: the C client.
 *
 * <p>{@code ProtoContractTest} in dlagent-grpc compares the .proto against the
 * Java descriptors and says so itself: "the other half, proto against
 * agent_c_api.h, lives on the C side". That half did not exist, and the gap is
 * not hypothetical -- {@code CleanupMetadata} and {@code DeleteFile} were
 * declared in the proto and implemented in Java for the whole migration while
 * the C client never called them, because it still spoke to those two
 * endpoints with raw libcurl. Under agent_transport=grpc every metadata
 * cleanup therefore failed, and in the subtransaction-abort path it took the
 * coordinator down with it.
 *
 * <p>Nothing in the build compared those two sides. This test does.
 *
 * <p>THE DIRECTION THAT MATTERS is C into proto: an operation the FDW can
 * invoke MUST have an RPC and a handler, or it fails at runtime with
 * UNIMPLEMENTED. The other direction is looser on purpose -- an RPC with no C
 * caller is dead weight, not a failure -- so it is asserted against a named
 * list instead, which keeps the existing slack visible and makes NEW slack a
 * build failure.
 */
class OperationCoverageTest {

    /** Where the sources live, relative to this module's directory. */
    private static final Path AGENT = Paths.get("..").toAbsolutePath().normalize();
    private static final Path FDW = AGENT.resolveSibling("datalake_fdw");

    /**
     * The C headers that declare the operation surface, BOTH of them.
     *
     * <p>agent_cli_wrapper.h includes agent_c_api.h and adds six more
     * declarations of its own; the live callers include the former. Reading
     * only one of the two would have missed {@code delete_file}.
     */
    private static final Path[] C_HEADERS = {
        FDW.resolve("src/components/agent_cli/c_interface/agent_c_api.h"),
        FDW.resolve("src/common/agent_cli_wrapper.h"),
    };

    private static final Path PROTO = AGENT.resolve("proto/dlagent.proto");

    /**
     * RPCs with no C caller, each with a reason.
     *
     * <p>CreateNamespace: dlagent implements it and the C++ client has the
     * method, but no {@code agent_cli_wrapper_create_namespace} exists, so the
     * FDW cannot reach it -- namespaces are created through
     * createCatalogWithNamespace instead. Harmless today; listed so that it
     * stays a deliberate gap rather than becoming one more unnoticed.
     */
    private static final Set<String> RPCS_WITHOUT_A_C_CALLER =
            new HashSet<>(Arrays.asList("CreateNamespace"));

    /** The data plane is a different boundary (libchurl), not an agent_cli operation. */
    private static final Set<String> DATA_PLANE =
            new HashSet<>(Arrays.asList("Read", "Write"));

    /**
     * An operation is a void agent_cli_wrapper_* taking a handle and a JSON
     * argument. That rules the utility functions out by shape rather than by a
     * blocklist, so a new helper does not have to be remembered here.
     */
    private static final Pattern C_OPERATION = Pattern.compile(
            "\\bvoid\\s+(agent_cli_wrapper_\\w+)\\s*\\(([^;]*?)\\)\\s*;", Pattern.DOTALL);

    private static final Pattern JSON_PARAM = Pattern.compile("\\bconst char\\s*\\*\\s*\\w*json\\w*");

    private static final Pattern RPC = Pattern.compile("^\\s*rpc\\s+(\\w+)", Pattern.MULTILINE);

    private static Set<String> cOperations() throws IOException {
        Set<String> operations = new TreeSet<>();
        for (Path header : C_HEADERS) {
            assertTrue(Files.isRegularFile(header), "missing C header: " + header);
            String source = new String(Files.readAllBytes(header), StandardCharsets.UTF_8);
            Matcher m = C_OPERATION.matcher(source);
            while (m.find()) {
                String params = m.group(2).replaceAll("\\s+", " ");
                if (params.contains("AgentCliHandle") && JSON_PARAM.matcher(params).find()) {
                    operations.add(pascal(m.group(1)));
                }
            }
        }
        return operations;
    }

    private static Set<String> protoControlPlane() throws IOException {
        String source = new String(Files.readAllBytes(PROTO), StandardCharsets.UTF_8);
        Set<String> rpcs = new TreeSet<>();
        Matcher m = RPC.matcher(source);
        while (m.find()) {
            if (!DATA_PLANE.contains(m.group(1))) {
                rpcs.add(m.group(1));
            }
        }
        return rpcs;
    }

    /** agent_cli_wrapper_load_metadata_json -> LoadMetadataJson */
    private static String pascal(String cName) {
        StringBuilder out = new StringBuilder();
        for (String part : cName.substring("agent_cli_wrapper_".length()).split("_")) {
            if (!part.isEmpty()) {
                out.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
        }
        return out.toString();
    }

    @Test
    @DisplayName("the C client parses into a non-trivial operation set")
    void theParseFindsSomething() throws IOException {
        Set<String> c = cOperations();
        assertTrue(c.size() > 15,
                "expected the C header to declare the agent's operations, found " + c);
        assertTrue(c.contains("DeleteFile") && c.contains("CleanupMetadata"),
                "the two file-cleanup operations must be part of the C surface: " + c);
    }

    @Test
    @DisplayName("every operation the C client can invoke has an RPC")
    void cOperationsAllHaveAnRpc() throws IOException {
        Set<String> missing = new LinkedHashSet<>(cOperations());
        missing.removeAll(protoControlPlane());
        assertTrue(missing.isEmpty(),
                "the FDW can call these, but dlagent.proto declares no RPC for them, so they "
                        + "would fail with UNIMPLEMENTED under agent_transport=grpc: " + missing);
    }

    @Test
    @DisplayName("every operation the C client can invoke is parity-tested")
    void cOperationsAreAllCovered() throws IOException {
        Set<String> covered = Arrays.stream(Op.values())
                .map(op -> pascal("agent_cli_wrapper_" + op.name().toLowerCase()))
                .collect(Collectors.toCollection(TreeSet::new));
        Set<String> missing = new LinkedHashSet<>(cOperations());
        missing.removeAll(covered);
        assertTrue(missing.isEmpty(),
                "these C operations have no entry in Op, so no parity test runs over them: "
                        + missing);
    }

    @Test
    @DisplayName("an RPC with no C caller must be a listed, deliberate gap")
    void unreachableRpcsAreAccountedFor() throws IOException {
        Set<String> unreachable = new TreeSet<>(protoControlPlane());
        unreachable.removeAll(cOperations());
        assertEquals(new TreeSet<>(RPCS_WITHOUT_A_C_CALLER), unreachable,
                "the set of RPCs no C caller can reach has changed. Add the C wrapper, or "
                        + "remove the RPC, or add it to RPCS_WITHOUT_A_C_CALLER with a reason");
    }
}
