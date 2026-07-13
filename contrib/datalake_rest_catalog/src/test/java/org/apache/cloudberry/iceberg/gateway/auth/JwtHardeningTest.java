package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Offline coverage for JWT signing-secret hardening (issue #382 C2 Task 2, decision J1):
 * empty secret WARNS by default, {@code jwt.requireConfiguredSecret=true} makes it fail-closed,
 * and a configured secret must decode to >= 32 bytes (HS256 wants >= 256 bit). No live DB needed —
 * {@link AuthPreflight#checkJwt(GatewayConfig)} is config-only.
 */
class JwtHardeningTest {

    private static GatewayConfig cfg(Map<String, String> overrides) {
        return GatewayConfig.forTest(overrides);
    }

    private static String base64OfLength(int numBytes) {
        byte[] raw = new byte[numBytes];
        return Base64.getEncoder().encodeToString(raw);
    }

    @Test
    void emptySecretWithRequireTrue_failsCheckJwt() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("jwt.hmacSecretBase64", "");
        overrides.put("jwt.requireConfiguredSecret", "true");

        AuthPreflight.Result r = AuthPreflight.checkJwt(cfg(overrides));

        assertThat(r.ok).isFalse();
        assertThat(r.reason).contains("jwt.hmacSecretBase64").contains("jwt.requireConfiguredSecret=true");
    }

    @Test
    void shortSecret_failsCheckJwt() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("jwt.hmacSecretBase64", base64OfLength(16));
        overrides.put("jwt.requireConfiguredSecret", "false");

        AuthPreflight.Result r = AuthPreflight.checkJwt(cfg(overrides));

        assertThat(r.ok).isFalse();
        assertThat(r.reason).contains("16").contains(">=32");
    }

    @Test
    void strongSecret_ok() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("jwt.hmacSecretBase64", base64OfLength(32));
        overrides.put("jwt.requireConfiguredSecret", "true");

        AuthPreflight.Result r = AuthPreflight.checkJwt(cfg(overrides));

        assertThat(r.ok).isTrue();
        assertThat(r.reason).isNull();
    }

    @Test
    void emptySecretWithRequireFalse_ok() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("jwt.hmacSecretBase64", "");
        overrides.put("jwt.requireConfiguredSecret", "false");

        AuthPreflight.Result r = AuthPreflight.checkJwt(cfg(overrides));

        assertThat(r.ok).isTrue();
        assertThat(r.reason).isNull();
    }
}
