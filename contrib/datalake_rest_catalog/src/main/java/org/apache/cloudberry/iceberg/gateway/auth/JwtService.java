package org.apache.cloudberry.iceberg.gateway.auth;

import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import javax.crypto.SecretKey;

/**
 * Issues and validates short-lived, self-signed JWTs (design §6.1/§6.4). The token carries the PG
 * username so subsequent requests only verify the signature + expiry instead of re-running SCRAM.
 *
 * <p>TODO(phase1): support key rotation (kid header + keyset) rather than a single HMAC secret.
 */
public final class JwtService {

    private final SecretKey key;
    private final long ttlSeconds;
    private final String issuer;

    public JwtService(GatewayConfig config) {
        String b64 = config.get("jwt.hmacSecretBase64", "");
        if (b64 == null || b64.isBlank()) {
            // Dev-only: ephemeral key, tokens die on restart. Configure a real secret in prod.
            this.key = Jwts.SIG.HS256.key().build();
        } else {
            byte[] decoded = Base64.getDecoder().decode(b64);
            // Belt-and-suspenders: AuthPreflight.checkJwt() should already have caught this at
            // startup, but never sign with a sub-256-bit HS256 key even if preflight was bypassed.
            if (decoded.length < 32) {
                throw new IllegalArgumentException("jwt.hmacSecretBase64 decodes to "
                        + decoded.length + " bytes; HS256 requires >=32");
            }
            this.key = Keys.hmacShaKeyFor(decoded);
        }
        this.ttlSeconds = config.getInt("jwt.ttlSeconds", 900);
        this.issuer = config.get("jwt.issuer", "pg-iceberg-rest-gateway");
    }

    /**
     * The configured token lifetime in seconds. The OAuth token response advertises this as
     * {@code expires_in} so clients refresh in step with the JWT {@code exp} claim minted below.
     */
    public long getTtlSeconds() {
        return ttlSeconds;
    }

    /** Mint a token whose subject is the authenticated PG username. */
    public String issue(String pgUser) {
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer(issuer)
                .subject(pgUser)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plusSeconds(ttlSeconds)))
                .signWith(key)
                .compact();
    }

    /**
     * Validate signature + expiry and return the PG username (subject).
     *
     * @throws io.jsonwebtoken.JwtException if invalid/expired -> caller maps to HTTP 401.
     */
    public String verifyAndGetUser(String token) {
        Jws<Claims> jws = Jwts.parser()
                .requireIssuer(issuer)
                .verifyWith(key)
                .build()
                .parseSignedClaims(token);
        return jws.getPayload().getSubject();
    }
}
