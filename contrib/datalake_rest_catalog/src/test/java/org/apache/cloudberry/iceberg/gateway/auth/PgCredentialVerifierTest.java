package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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

    /** A pg_hba path that checks passwords: only alice/right gets in. */
    private static PgCredentialVerifier.Login passwordChecked(List<String> attempts) {
        return (user, password) -> {
            attempts.add(password);
            if (!"alice".equals(user) || !"right".equals(password)) {
                throw new SQLException("password authentication failed", "28P01");
            }
            return false;
        };
    }

    @Test
    void rightPasswordOnAPasswordCheckedPathIsAccepted() {
        List<String> attempts = new ArrayList<>();
        assertThat(new PgCredentialVerifier(passwordChecked(attempts)).verify("alice", "right")).isTrue();
        // The real login, then the canary with a different, wrong password.
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(1)).isNotEqualTo("right");
    }

    @Test
    void theCanarySharesNothingWithTheRealPassword() {
        // A backend that truncates passwords (LDAP {CRYPT} keeps 8 chars) must not see a prefix
        // match, and the real password must not be sent to it a second time.
        List<String> attempts = new ArrayList<>();
        new PgCredentialVerifier(passwordChecked(attempts)).verify("alice", "right");
        assertThat(attempts.get(1)).doesNotContain("right");
    }

    @Test
    void aCanaryFailingForAnyReasonButAuthenticationRefusesTheToken() {
        // Trust path: the real login gets in; the canary then fails for a non-auth reason that a
        // flood of token requests can provoke. That proves nothing, so no token.
        for (String state : new String[]{"53300", "57P03", "08001", null}) {
            List<String> attempts = new ArrayList<>();
            PgCredentialVerifier v = new PgCredentialVerifier((user, password) -> {
                attempts.add(password);
                if (attempts.size() == 2) {
                    throw new SQLException("not an auth failure", state);
                }
                return false;
            });
            assertThat(v.verify("alice", "anything")).as(String.valueOf(state)).isFalse();
            assertThat(attempts).hasSize(2);
        }
    }

    @Test
    void aPgHbaRejectAlsoCountsAsPasswordChecked() {
        List<String> attempts = new ArrayList<>();
        PgCredentialVerifier v = new PgCredentialVerifier((user, password) -> {
            attempts.add(password);
            if (attempts.size() == 2) {
                throw new SQLException("pg_hba.conf rejects connection", "28000");
            }
            return false;
        });
        assertThat(v.verify("alice", "right")).isTrue();
    }

    @Test
    void wrongPasswordIsRejectedWithoutACanaryLogin() {
        List<String> attempts = new ArrayList<>();
        assertThat(new PgCredentialVerifier(passwordChecked(attempts)).verify("alice", "guess")).isFalse();
        assertThat(attempts).containsExactly("guess");
    }

    @Test
    void aTrustPathIsRefusedEvenWithTheRightPassword() {
        // host all all 127.0.0.1/24 trust: every password logs in, so none of them proves anything.
        List<String> attempts = new ArrayList<>();
        PgCredentialVerifier trust = new PgCredentialVerifier((user, password) -> {
            attempts.add(password);
            return false;
        });
        assertThat(trust.verify("alice", "right")).isFalse();
        // Refused because the canary got in, not for some other reason.
        assertThat(attempts).hasSize(2);
        assertThat(trust.verify("alice", "anything")).isFalse();
    }

    @Test
    void superuserIsRefusedBeforeTheCanary() {
        List<String> attempts = new ArrayList<>();
        PgCredentialVerifier su = new PgCredentialVerifier((user, password) -> {
            attempts.add(password);
            return true;
        });
        assertThat(su.verify("gpadmin", "right")).isFalse();
        assertThat(attempts).containsExactly("right");
    }
}
