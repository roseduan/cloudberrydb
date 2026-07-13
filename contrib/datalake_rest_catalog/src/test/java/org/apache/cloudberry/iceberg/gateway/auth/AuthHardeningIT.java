package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

/**
 * Live checks against lightning-382. Skips without PG_TEST_JDBC_URL.
 * Requires seed_builtin.sql roles (iceberg_reader has SELECT, no_access none) and that
 * only gpadmin has a trust HBA line (cluster state 2026-07-07).
 */
class AuthHardeningIT {

    private static String url() { return System.getenv("PG_TEST_JDBC_URL"); }

    @Test
    void superuserSubjectIsRefused() {
        assumeTrue(url() != null && !url().isBlank(), "no live cluster");
        // gpadmin is superuser + trust -> would connect, but must be REFUSED as a token subject.
        PgCredentialVerifier v = new PgCredentialVerifier(url());
        assertThat(v.verify("gpadmin", "any-garbage")).isFalse();
    }

    @Test
    void nonSuperuserReaderWithRealPasswordIsAccepted() {
        assumeTrue(url() != null && !url().isBlank(), "no live cluster");
        PgCredentialVerifier v = new PgCredentialVerifier(url());
        // iceberg_reader / reader_pw are seeded; must authenticate (needs a password HBA line
        // for this role — see D1 hardened pg_hba fragment).
        assertThat(v.verify("iceberg_reader", "reader_pw")).isTrue();
        assertThat(v.verify("iceberg_reader", "wrong_pw")).isFalse();
    }

    @Test
    void preflightAbortsOnSuperuserAuthenticator() {
        assumeTrue(url() != null && !url().isBlank(), "no live cluster");
        // gpadmin as authenticator must be rejected (superuser AND trust-detectable).
        AuthPreflight.Result r = AuthPreflight.check(url(), "gpadmin", "");
        assertThat(r.ok).isFalse();
        assertThat(r.reason).isNotBlank();
    }
}
