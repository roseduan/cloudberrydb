package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PgCredentialVerifierTest {

    // A bogus URL that would fail to connect if reached — proves empty/null passwords
    // short-circuit to false BEFORE any connection attempt (design: sub-project C, P0-1).
    private final PgCredentialVerifier verifier =
            new PgCredentialVerifier("jdbc:postgresql://127.0.0.1:1/nope?connectTimeout=1&loginTimeout=1");

    @Test
    void emptyPasswordIsRejectedWithoutConnecting() {
        assertThat(verifier.verify("gpadmin", "")).isFalse();
    }

    @Test
    void nullPasswordIsRejected() {
        assertThat(verifier.verify("gpadmin", null)).isFalse();
    }

    @Test
    void nullOrEmptyUserIsRejected() {
        assertThat(verifier.verify("", "pw")).isFalse();
        assertThat(verifier.verify(null, "pw")).isFalse();
    }
}
