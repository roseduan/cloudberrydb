package cloud.elastic.dlagent.plugins.iceberg;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * The post-restart backstop only works if the token it looks for was written.
 *
 * <p>alreadyCommitted() decides whether a resent commit has already been
 * applied by scanning snapshot summaries for this request's txn id.  That
 * search can only ever succeed on a path that also calls stampTxnId(), which
 * is what puts the id into the summary in the first place.  commitRewrite()
 * once had the check without the stamp: the guard compiled, ran, and returned
 * false every time, so a VACUUM commit resent after an agent restart was not
 * protected at all.  Nothing failed, which is why it survived until review.
 *
 * <p>This reads the source rather than exercising the commits because the
 * commit paths need a live catalog, a table and a fragment list to reach, and
 * the module has no harness that builds one.  A behavioural test is the better
 * test and should replace this once such a harness exists; until then the
 * invariant is worth holding onto in the cheap form, because the failure mode
 * is a guard that silently does nothing.
 */
class CommitTokenSymmetryTest {

    /*
     * Both invariants here are the same mistake in two places: a value is
     * threaded into a call site and then never produced, so the machinery
     * that consumes it runs against nothing.  Neither failure throws, logs,
     * or changes a result -- one leaves a duplicate-commit guard permanently
     * false, the other leaves a prefetch permanently empty and the commit
     * reading footers one at a time.  That silence is what earns them a test.
     */

    private static final String SOURCE =
            "src/main/java/cloud/elastic/dlagent/plugins/iceberg/IcebergMetadataFetcher.java";

    /** Start of a method whose body we want to look at as a unit. */
    private static final Pattern METHOD =
            Pattern.compile("^\\s{4}(?:public|private|protected)\\s+.*\\b(\\w+)\\s*\\(");

    @Test
    void everyPathThatChecksTheTokenAlsoWritesIt() throws IOException {
        Path source = Paths.get(SOURCE);
        assumeTrue(Files.exists(source), "source not reachable from the test working directory");

        List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);

        String current = null;
        StringBuilder body = new StringBuilder();
        List<String> offenders = new ArrayList<>();

        for (String line : lines) {
            Matcher m = METHOD.matcher(line);
            if (m.find()) {
                check(current, body.toString(), offenders);
                current = m.group(1);
                body.setLength(0);
            }
            body.append(line).append('\n');
        }
        check(current, body.toString(), offenders);

        assertTrue(offenders.isEmpty(),
                   "these methods consult alreadyCommitted() but never call stampTxnId(), so the "
                   + "token they look for is never written and the guard can only return false: "
                   + offenders);
    }

    @Test
    void everyPathThatPassesThePrefetchAlsoFillsIt() throws IOException {
        Path source = Paths.get(SOURCE);
        assumeTrue(Files.exists(source), "source not reachable from the test working directory");

        List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);

        String current = null;
        StringBuilder body = new StringBuilder();
        List<String> offenders = new ArrayList<>();

        for (String line : lines) {
            Matcher m = METHOD.matcher(line);
            if (m.find()) {
                checkPrefetch(current, body.toString(), offenders);
                current = m.group(1);
                body.setLength(0);
            }
            body.append(line).append('\n');
        }
        checkPrefetch(current, body.toString(), offenders);

        assertTrue(offenders.isEmpty(),
                   "these methods hand prefetchedMetrics to a manifest-entry builder but never "
                   + "fill it, so the map stays empty and every footer they need is read one at "
                   + "a time: " + offenders);
    }

    /**
     * The map used as an argument, which is every way of handing it to a
     * builder and no way of producing it: the field's declaration and the
     * assignment inside the prefetch helpers are not preceded by "(" or ",".
     * Matching the argument rather than naming the four builders is what
     * keeps a fifth builder from slipping past this.
     */
    private static final Pattern PASSED = Pattern.compile("[(,]\\s*prefetchedMetrics\\b");

    private static void checkPrefetch(String method, String body, List<String> offenders) {
        if (method == null || method.startsWith("prefetchFallbackMetrics")) {
            return;
        }
        if (PASSED.matcher(body).find() && !body.contains("prefetchFallbackMetrics")) {
            offenders.add(method);
        }
    }

    private static void check(String method, String body, List<String> offenders) {
        if (method == null) {
            return;
        }
        /* The two helpers name themselves; only their callers are of interest. */
        if ("alreadyCommitted".equals(method) || "stampTxnId".equals(method)) {
            return;
        }
        if (body.contains("alreadyCommitted(") && !body.contains("stampTxnId(")) {
            offenders.add(method);
        }
    }
}
