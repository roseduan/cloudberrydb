/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.grpc.DlAgentServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The message-size default, spelled in three languages, must be one number.
 *
 * <p>gRPC enforces a message limit on whichever side RECEIVES, so the client
 * and the server each carry their own ceiling. The live value travels --
 * datalake_proxy reads the {@code datalake.rpc_max_message_size} GUC and puts
 * it on the agent's command line -- but each side also needs a DEFAULT for
 * the case where it is started without one, and those three defaults are in
 * three files the compiler cannot compare:
 *
 * <ul>
 * <li>{@code dlagent_transport.h}, the GUC's own default;
 * <li>{@code grpc_transport.cpp}, the C++ client's fallback, which cannot
 *     include that header because it must stay free of postgres.h;
 * <li>{@link DlAgentServer}, the agent's fallback when launched by hand.
 * </ul>
 *
 * <p>Letting them drift reintroduces exactly the bug the setting was added
 * for: a client that may send more than the server will receive, failing
 * under grpc for a request rest accepts. {@code grpc_transport.cpp} says in
 * its own comment that the two "are checked against each other by the
 * regression suite, not by the compiler". This is that check.
 */
class MessageSizeDefaultTest {

    private static final Path AGENT = Paths.get("..").toAbsolutePath().normalize();
    private static final Path FDW = AGENT.resolveSibling("datalake_fdw");

    private static String read(Path p) throws IOException {
        assertTrue(Files.exists(p), "expected to find " + p);
        return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
    }

    private static int match(String source, String regex, String what) {
        Matcher m = Pattern.compile(regex).matcher(source);
        assertTrue(m.find(), "could not find " + what
                + " -- if it was renamed, this test has to follow it");
        return Integer.parseInt(m.group(1));
    }

    @Test
    @DisplayName("the GUC default, the C++ fallback and the agent fallback are one number")
    void defaultsAgree() throws IOException {
        int gucDefaultMb = match(
                read(AGENT.resolve("dlagent_transport.h")),
                "#define\\s+DATALAKE_AGENT_DEFAULT_RPC_MAX_MESSAGE_SIZE_MB\\s+(\\d+)",
                "DATALAKE_AGENT_DEFAULT_RPC_MAX_MESSAGE_SIZE_MB in dlagent_transport.h");

        int cppFallbackMb = match(
                read(FDW.resolve("src/components/agent_cli/grpc/grpc_transport.cpp")),
                "DEFAULT_MAX_MESSAGE_SIZE\\s*=\\s*(\\d+)\\s*\\*\\s*1024\\s*\\*\\s*1024",
                "DEFAULT_MAX_MESSAGE_SIZE in grpc_transport.cpp");

        assertEquals(gucDefaultMb, cppFallbackMb,
                "the C++ client's fallback must equal the GUC default");
        assertEquals(gucDefaultMb, DlAgentServer.DEFAULT_MAX_MESSAGE_SIZE_MB,
                "the agent's fallback must equal the GUC default");
    }

    /**
     * And every default must clear gRPC's own 4 MiB, or the setting would be
     * cosmetic: that default is the thing being worked around.
     */
    @Test
    @DisplayName("the shared default is above gRPC's 4 MiB")
    void defaultClearsTheGrpcDefault() {
        assertTrue(DlAgentServer.DEFAULT_MAX_MESSAGE_SIZE_MB * 1024 * 1024 > 4 * 1024 * 1024,
                "a default at or below 4 MiB leaves grpc's own limit in charge");
    }

    /**
     * The GUC's upper bound has to stay inside a 32-bit byte count, because
     * both grpc++ and grpc-java take the limit as an int.
     */
    @Test
    @DisplayName("the GUC's maximum stays inside INT_MAX bytes")
    void maximumFitsInAnInt() throws IOException {
        int maxMb = match(
                read(AGENT.resolve("dlagent_transport.h")),
                "#define\\s+DATALAKE_AGENT_MAX_RPC_MAX_MESSAGE_SIZE_MB\\s+(\\d+)",
                "DATALAKE_AGENT_MAX_RPC_MAX_MESSAGE_SIZE_MB in dlagent_transport.h");

        assertTrue((long) maxMb * 1024 * 1024 <= Integer.MAX_VALUE,
                maxMb + " MB does not fit in an int32 byte count");
    }
}
