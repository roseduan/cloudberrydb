package cloud.elastic.dlagent.api.filter;

import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import cloud.elastic.dlagent.plugins.hudi.ColumnPruner;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The pruned filter must never be stricter than the original one: data
 * skipped by it is never read.  Dropping an operand of an AND only widens
 * the filter, but under a NOT it narrows it.
 */
class BaseTreePrunerTest {

    /* Everything but LIKE, as the Iceberg plugin supports. */
    private static final EnumSet<Operator> NO_LIKE = EnumSet.complementOf(EnumSet.of(Operator.LIKE));

    /* a0 = 1, a1 LIKE 'x%', a2 = 3 */
    private static final String EQ_A0 = "a0c23s1d1o5";
    private static final String LIKE_A1 = "a1c25s2dx%o7";
    private static final String EQ_A2 = "a2c23s1d3o5";

    private static Node prune(String filter, TreeVisitor pruner) throws Exception {
        return new TreeTraverser().traverse(new FilterParser().parse(filter), pruner);
    }

    private static String render(Node node) {
        if (node == null) {
            return null;
        }
        ToStringTreeVisitor visitor = new ToStringTreeVisitor();
        new TreeTraverser().traverse(node, visitor);
        return visitor.toString();
    }

    private static String parsed(String filter) throws Exception {
        return render(new FilterParser().parse(filter));
    }

    @Test
    void notOverAndWithUnsupportedOperandIsDropped() throws Exception {
        // NOT (a0 = 1 AND a1 LIKE 'x%') must not become NOT (a0 = 1)
        assertNull(prune(EQ_A0 + LIKE_A1 + "l0l2", new SupportedOperatorPruner(NO_LIKE)));
    }

    @Test
    void notDroppedButSiblingsUnderAndKept() throws Exception {
        // NOT (a0 = 1 AND a1 LIKE 'x%') AND a2 = 3  ->  a2 = 3
        Node result = prune(EQ_A0 + LIKE_A1 + "l0l2" + EQ_A2 + "l0", new SupportedOperatorPruner(NO_LIKE));
        assertEquals(parsed(EQ_A2), render(result));
    }

    @Test
    void notWithFullySupportedSubtreeIsKept() throws Exception {
        String filter = EQ_A0 + "a1c23s1d2o6l0l2";
        assertEquals(parsed(filter), render(prune(filter, new SupportedOperatorPruner(NO_LIKE))));
    }

    @Test
    void andStillDropsUnsupportedOperand() throws Exception {
        // Outside a NOT, dropping an AND operand only widens the filter
        Node result = prune(EQ_A0 + LIKE_A1 + "l0", new SupportedOperatorPruner(NO_LIKE));
        assertEquals(parsed(EQ_A0), render(result));
    }

    @Test
    void orWithUnsupportedOperandIsDropped() throws Exception {
        assertNull(prune(EQ_A0 + LIKE_A1 + "l1", new SupportedOperatorPruner(NO_LIKE)));
    }

    @Test
    void partitionPrunerDropsNotOverMixedAnd() throws Exception {
        // Hudi partition pruning keeps partition-column predicates only;
        // NOT (p = 1 AND x = 2) must not become NOT (p = 1)
        List<ColumnDescriptor> columns = Arrays.asList(
                new ColumnDescriptor("p", 23, 0, "int4", null),
                new ColumnDescriptor("x", 23, 1, "int4", null));
        ColumnPruner pruner = new ColumnPruner(EnumSet.allOf(Operator.class),
                Collections.singleton("p"), columns, true);
        assertNull(prune(EQ_A0 + "a1c23s1d2o5l0l2", pruner));
    }
}
