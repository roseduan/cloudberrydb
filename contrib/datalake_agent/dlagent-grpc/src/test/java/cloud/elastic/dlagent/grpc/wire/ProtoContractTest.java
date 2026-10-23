/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.wire;

import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Keeps {@link DlagentMethods} and {@code proto/dlagent.proto} from drifting.
 *
 * <p>The C++ client generates its stubs from the .proto; the Java server
 * declares its descriptors by hand. Nothing in the build forces those two to
 * agree, so this test does: it parses the contract and compares the method
 * set, the owning service and the streaming-ness of every RPC. A method added
 * to one side only would otherwise show up as UNIMPLEMENTED at runtime, and a
 * unary/streaming mismatch as a hang.
 *
 * <p>This is the Java half of keeping operation coverage complete and not
 * drifting; the other half, proto against
 * {@code agent_c_api.h}, lives on the C side.
 */
public class ProtoContractTest {

    private static final Pattern SERVICE = Pattern.compile("^service\\s+(\\w+)\\s*\\{");
    private static final Pattern RPC =
            Pattern.compile("^\\s*rpc\\s+(\\w+)\\s*\\(\\s*(stream\\s+)?Request\\s*\\)\\s*returns\\s*\\(\\s*(stream\\s+)?Response\\s*\\)");

    private static Path protoFile() {
        // The test runs with the module directory as cwd.
        return Paths.get("..", "proto", "dlagent.proto").toAbsolutePath().normalize();
    }

    /** full method name -> streaming?, as declared in the .proto. */
    private Map<String, Boolean> parseProto() throws IOException {
        Map<String, Boolean> out = new LinkedHashMap<>();
        String pkg = null;
        String service = null;
        for (String raw : Files.readAllLines(protoFile(), StandardCharsets.UTF_8)) {
            String line = raw.trim();
            if (line.startsWith("//")) {
                continue;
            }
            if (line.startsWith("package ")) {
                pkg = line.substring("package ".length()).replace(";", "").trim();
            }
            Matcher sm = SERVICE.matcher(line);
            if (sm.find()) {
                service = sm.group(1);
                continue;
            }
            if (line.equals("}")) {
                service = null;
                continue;
            }
            Matcher rm = RPC.matcher(raw);
            if (rm.find() && service != null) {
                assertTrue(rm.group(2) == null,
                        "client streaming is not part of this contract: " + rm.group(1));
                out.put(pkg + "." + service + "/" + rm.group(1), rm.group(3) != null);
            }
        }
        return out;
    }

    @Test
    public void javaDescriptorsMatchTheProtoContract() throws IOException {
        Map<String, Boolean> proto = parseProto();
        assertTrue(proto.size() >= 20,
                "parsed only " + proto.size() + " rpcs from " + protoFile()
                        + "; the parser or the file path is wrong, and a test that "
                        + "checks nothing would pass forever");

        Map<String, Boolean> java = new LinkedHashMap<>();
        for (Map.Entry<String, MethodDescriptor<byte[], byte[]>> e : DlagentMethods.all().entrySet()) {
            java.put(e.getKey(),
                    e.getValue().getType() == MethodDescriptor.MethodType.SERVER_STREAMING);
        }

        assertEquals(new TreeSet<>(proto.keySet()), new TreeSet<>(java.keySet()),
                "the set of RPCs in proto/dlagent.proto and in DlagentMethods diverged. "
                        + "A method declared on only one side is UNIMPLEMENTED at runtime.");

        for (String name : proto.keySet()) {
            assertEquals(proto.get(name), java.get(name),
                    name + ": the proto and DlagentMethods disagree on whether it streams. "
                            + "A unary/streaming mismatch does not fail fast, it hangs.");
        }
    }

    /**
     * {@code google.protobuf.Struct} silently truncates int64 through its
     * double-typed number_value, and Iceberg metadata is full of random int64
     * snapshot ids. The contract bans it (see the header of dlagent.proto);
     * this makes the ban enforceable rather than advisory.
     */
    @Test
    public void contractDoesNotUseStruct() throws IOException {
        String text = new String(Files.readAllBytes(protoFile()), StandardCharsets.UTF_8);
        StringBuilder offenders = new StringBuilder();
        for (String raw : text.split("\n")) {
            String line = raw.trim();
            if (line.startsWith("//")) {
                continue;
            }
            if (line.contains("google/protobuf/struct.proto") || line.contains("google.protobuf.Struct")) {
                offenders.append("\n  ").append(line);
            }
        }
        assertEquals("", offenders.toString(),
                "google.protobuf.Struct appeared in the contract. Its Value.number_value "
                        + "is a double, so any int64 passing through it (every Iceberg "
                        + "snapshot id) is silently corrupted.");
    }
}
