/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.grpc.DlAgentServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The C++ client retries an UNAVAILABLE only when its description is the
 * readiness gate's, because only that call provably did not run. If the two
 * strings drift apart the client stops retrying the startup window and the
 * first query after an agent restart fails.
 */
class NotReadyContractTest {

    private static final Path CLIENT = Paths.get("..").toAbsolutePath().normalize()
            .resolveSibling("datalake_fdw")
            .resolve("src/components/agent_cli/grpc/grpc_agent_client.cpp");

    @Test
    void theClientRetriesExactlyWhatTheGateSays() throws IOException {
        String source = new String(Files.readAllBytes(CLIENT), StandardCharsets.UTF_8);
        String expected = "NOT_READY_DESCRIPTION = \"" + DlAgentServer.NOT_READY_DESCRIPTION + "\"";
        assertTrue(source.contains(expected),
                "grpc_agent_client.cpp must define " + expected);
    }
}
