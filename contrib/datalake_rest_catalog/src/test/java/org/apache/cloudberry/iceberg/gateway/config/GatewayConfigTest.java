package org.apache.cloudberry.iceberg.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Smoke test proving the build + test wiring works. Replace with real coverage in Phase 1. */
class GatewayConfigTest {

    @Test
    void loadsBundledDefaults() {
        GatewayConfig config = GatewayConfig.load();
        assertThat(config.getInt("server.port", -1)).isEqualTo(8181);
    }
}
