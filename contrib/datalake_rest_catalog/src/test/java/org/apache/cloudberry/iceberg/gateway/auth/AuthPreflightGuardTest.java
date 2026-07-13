package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Offline coverage for the missing-credential guard in {@link AuthPreflight#check}: a null or
 * blank pg.authenticator.user (and a null password) must surface as a structured
 * {@code Result(false, ...)} preflight failure, not as the NullPointerException that
 * {@code Properties.setProperty(..., null)} would otherwise throw inside {@code canConnect}.
 * No live DB needed — the guard returns before any connection attempt.
 */
class AuthPreflightGuardTest {

    private static final String URL = "jdbc:postgresql://127.0.0.1:1/postgres";

    @Test
    void nullUser_failsStructurally() {
        AuthPreflight.Result r = AuthPreflight.check(URL, null, "pw");
        assertThat(r.ok).isFalse();
        assertThat(r.reason).contains("pg.authenticator.user");
    }

    @Test
    void blankUser_failsStructurally() {
        AuthPreflight.Result r = AuthPreflight.check(URL, "   ", "pw");
        assertThat(r.ok).isFalse();
        assertThat(r.reason).contains("pg.authenticator.user");
    }

    @Test
    void nullPassword_failsStructurally() {
        AuthPreflight.Result r = AuthPreflight.check(URL, "iceberg_authenticator", null);
        assertThat(r.ok).isFalse();
        assertThat(r.reason).contains("pg.authenticator.password");
    }
}
