/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent.plugins.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RESTException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommitStateGuardTest {

    /* ---------- wrap(): exception classification on commit ---------- */

    private TableOperations guardedOpsThrowing(RuntimeException failure) {
        TableOperations inner = mock(TableOperations.class);
        doThrow(failure).when(inner).commit(any(), any());

        Table table = mock(Table.class,
                org.mockito.Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        when(((HasTableOperations) table).operations()).thenReturn(inner);
        when(table.name()).thenReturn("t");

        Table guarded = CommitStateGuard.wrap(table);
        return ((HasTableOperations) guarded).operations();
    }

    @Test
    void bareRestExceptionBecomesCommitStateUnknown() {
        RESTException transport = new RESTException("Error occurred while processing POST request");
        TableOperations ops = guardedOpsThrowing(transport);

        CommitStateUnknownException e = assertThrows(CommitStateUnknownException.class,
                () -> ops.commit(null, null));
        assertEquals(transport, e.getCause());
    }

    @Test
    void classifiedRestSubclassPassesThrough() {
        TableOperations ops = guardedOpsThrowing(new BadRequestException("bad request"));

        assertThrows(BadRequestException.class, () -> ops.commit(null, null));
    }

    @Test
    void nonRestExceptionPassesThrough() {
        TableOperations ops = guardedOpsThrowing(new CommitFailedException("conflict"));

        assertThrows(CommitFailedException.class, () -> ops.commit(null, null));
    }

    @Test
    void wrapKeepsTableWithoutOperationsUntouched() {
        Table plain = mock(Table.class);
        assertEquals(plain, CommitStateGuard.wrap(plain));
    }

    /*
     * The guard sits on every catalog type, not just REST, so it must be
     * invisible to the paths that never see a lost reply: a commit that
     * succeeds must still succeed, and every other TableOperations call must
     * reach the original operations object untouched.
     */

    @Test
    void successfulCommitIsUnaffected() {
        TableOperations inner = mock(TableOperations.class);
        Table table = mock(Table.class,
                org.mockito.Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        when(((HasTableOperations) table).operations()).thenReturn(inner);
        when(table.name()).thenReturn("t");

        TableOperations guarded = ((HasTableOperations) CommitStateGuard.wrap(table)).operations();
        TableMetadata base = mock(TableMetadata.class);
        TableMetadata updated = mock(TableMetadata.class);
        guarded.commit(base, updated);

        org.mockito.Mockito.verify(inner).commit(base, updated);
    }

    @Test
    void everyOperationDelegatesToTheOriginal() {
        TableOperations inner = mock(TableOperations.class);
        TableMetadata current = mock(TableMetadata.class);
        TableMetadata refreshed = mock(TableMetadata.class);
        org.apache.iceberg.io.FileIO io = mock(org.apache.iceberg.io.FileIO.class);
        org.apache.iceberg.encryption.EncryptionManager em =
                mock(org.apache.iceberg.encryption.EncryptionManager.class);
        org.apache.iceberg.io.LocationProvider lp =
                mock(org.apache.iceberg.io.LocationProvider.class);
        TableOperations temp = mock(TableOperations.class);
        when(inner.current()).thenReturn(current);
        when(inner.refresh()).thenReturn(refreshed);
        when(inner.io()).thenReturn(io);
        when(inner.encryption()).thenReturn(em);
        when(inner.metadataFileLocation("f.json")).thenReturn("s3://b/metadata/f.json");
        when(inner.locationProvider()).thenReturn(lp);
        when(inner.temp(current)).thenReturn(temp);
        when(inner.newSnapshotId()).thenReturn(4242L);

        Table table = mock(Table.class,
                org.mockito.Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        when(((HasTableOperations) table).operations()).thenReturn(inner);
        when(table.name()).thenReturn("t");

        TableOperations guarded = ((HasTableOperations) CommitStateGuard.wrap(table)).operations();

        assertEquals(current, guarded.current());
        assertEquals(refreshed, guarded.refresh());
        assertEquals(io, guarded.io());
        assertEquals(em, guarded.encryption());
        assertEquals("s3://b/metadata/f.json", guarded.metadataFileLocation("f.json"));
        assertEquals(lp, guarded.locationProvider());
        assertEquals(temp, guarded.temp(current));
        assertEquals(4242L, guarded.newSnapshotId());
    }

    @Test
    void wrappedTableKeepsItsName() {
        TableOperations inner = mock(TableOperations.class);
        Table table = mock(Table.class,
                org.mockito.Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        when(((HasTableOperations) table).operations()).thenReturn(inner);
        when(table.name()).thenReturn("hivedb.orders");

        assertEquals("hivedb.orders", CommitStateGuard.wrap(table).name());
    }

    /* ---------- commitLanded(): learning the outcome from the table ---------- */

    /**
     * Builds a table whose snapshot chain is snapshots[0] <- snapshots[1] <- ...
     * (last element is the current head), where each snapshot added exactly
     * one data file with the given path.
     */
    private Table tableWithChain(String... addedPathPerSnapshot) {
        Table table = mock(Table.class);
        Map<Long, Snapshot> byId = new HashMap<>();
        Snapshot previous = null;
        for (int i = 0; i < addedPathPerSnapshot.length; i++) {
            Snapshot snapshot = mock(Snapshot.class);
            long id = 100L + i;
            Long parentId = previous == null ? null : (id - 1);
            when(snapshot.snapshotId()).thenReturn(id);
            when(snapshot.parentId()).thenReturn(parentId);
            DataFile file = mock(DataFile.class);
            when(file.path()).thenReturn(addedPathPerSnapshot[i]);
            when(snapshot.addedDataFiles(any())).thenReturn(Collections.singletonList(file));
            when(snapshot.addedDeleteFiles(any())).thenReturn(Collections.emptyList());
            byId.put(id, snapshot);
            previous = snapshot;
        }
        Snapshot head = previous;
        when(table.currentSnapshot()).thenReturn(head);
        when(table.snapshot(org.mockito.ArgumentMatchers.anyLong()))
                .thenAnswer(inv -> byId.get(inv.getArgument(0, Long.class)));
        return table;
    }

    private Set<String> paths(String... p) {
        return new HashSet<>(java.util.Arrays.asList(p));
    }

    @Test
    void landedWhenHeadAddedOurFiles() {
        Table t = tableWithChain("s3://b/data/old.par", "s3://b/data/ours.par");
        Snapshot base = t.snapshot(100L);

        assertTrue(CommitStateGuard.commitLanded(t, base, paths("s3://b/data/ours.par")));
    }

    @Test
    void landedWhenOurCommitIsBelowConcurrentOnes() {
        Table t = tableWithChain("s3://b/data/old.par", "s3://b/data/ours.par", "s3://b/data/theirs.par");
        Snapshot base = t.snapshot(100L);

        assertTrue(CommitStateGuard.commitLanded(t, base, paths("s3://b/data/ours.par")));
    }

    @Test
    void notLandedWhenChainDownToBaseLacksOurFiles() {
        Table t = tableWithChain("s3://b/data/old.par", "s3://b/data/theirs.par");
        Snapshot base = t.snapshot(100L);

        assertFalse(CommitStateGuard.commitLanded(t, base, paths("s3://b/data/ours.par")));
    }

    @Test
    void notLandedOnUnchangedHead() {
        Table t = tableWithChain("s3://b/data/old.par");
        Snapshot base = t.snapshot(100L);

        assertFalse(CommitStateGuard.commitLanded(t, base, paths("s3://b/data/ours.par")));
    }

    @Test
    void emptyTableStillEmptyMeansNotLanded() {
        Table t = mock(Table.class);
        when(t.currentSnapshot()).thenReturn(null);

        assertFalse(CommitStateGuard.commitLanded(t, null, paths("s3://b/data/ours.par")));
    }

    @Test
    void landedOnPreviouslyEmptyTable() {
        Table t = tableWithChain("s3://b/data/ours.par");

        assertTrue(CommitStateGuard.commitLanded(t, null, paths("s3://b/data/ours.par")));
    }

    @Test
    void inconclusiveWhenBaseNotOnChain() {
        Table t = tableWithChain("s3://b/data/theirs.par");
        Snapshot vanishedBase = mock(Snapshot.class);
        when(vanishedBase.snapshotId()).thenReturn(999L);

        assertNull(CommitStateGuard.commitLanded(t, vanishedBase, paths("s3://b/data/ours.par")));
    }

    @Test
    void inconclusiveWhenChainIsTruncated() {
        Table t = tableWithChain("s3://b/data/theirs.par");
        Snapshot head = t.currentSnapshot();
        when(head.parentId()).thenReturn(555L);   /* parent expired out of metadata */
        Snapshot base = mock(Snapshot.class);
        when(base.snapshotId()).thenReturn(998L);

        assertNull(CommitStateGuard.commitLanded(t, base, paths("s3://b/data/ours.par")));
    }

    @Test
    void deleteFilesCountAsLanded() {
        Table t = tableWithChain("s3://b/data/old.par");
        Snapshot head = t.currentSnapshot();
        org.apache.iceberg.DeleteFile del = mock(org.apache.iceberg.DeleteFile.class);
        when(del.path()).thenReturn("s3://b/data/ours-delete.par");
        when(head.addedDeleteFiles(any())).thenReturn(Collections.singletonList(del));
        Snapshot base = mock(Snapshot.class);
        when(base.snapshotId()).thenReturn(1L);
        when(head.parentId()).thenReturn(1L);

        assertTrue(CommitStateGuard.commitLanded(t, base, paths("s3://b/data/ours-delete.par")));
    }
}
