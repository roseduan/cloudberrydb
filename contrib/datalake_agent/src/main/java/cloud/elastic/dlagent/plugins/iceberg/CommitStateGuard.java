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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.Set;

/**
 * Keeps a commit whose outcome is unknowable from being treated as a clean
 * failure.
 *
 * When the connection to a REST catalog dies before any response arrives, the
 * REST client surfaces a bare {@link RESTException}.  For a commit that is the
 * one case where the server may well have applied the change, and treating it
 * as a definite failure is wrong in both directions: SnapshotProducer deletes
 * the metadata files the (possibly successful) commit now references —
 * corrupting the table for every reader — and callers re-run a statement whose
 * data may already be in the table.
 *
 * {@link #wrap(Table)} converts that one case into
 * {@link CommitStateUnknownException}, which SnapshotProducer already knows to
 * treat conservatively (no cleanup, no retry).  {@link #commitLanded} then
 * lets the caller learn the actual outcome from the table itself.
 */
final class CommitStateGuard {

    private CommitStateGuard() {
    }

    /**
     * Returns a view of {@code table} whose commit path reports a lost reply
     * as {@link CommitStateUnknownException} instead of a definite failure.
     */
    static Table wrap(Table table) {
        if (!(table instanceof HasTableOperations)) {
            return table;
        }
        TableOperations ops = ((HasTableOperations) table).operations();
        return new BaseTable(new GuardedOperations(ops), table.name());
    }

    /**
     * Decides whether a commit whose reply was lost actually took effect.
     *
     * The written data/delete files are UUID-named, so they can only have
     * entered the table through that commit: walk the snapshot chain from the
     * fresh head back to the snapshot the commit was based on and look for
     * them.
     *
     * @param fresh    the table, re-loaded from the catalog after the failure
     * @param base     the current snapshot before the commit (null for an
     *                 empty table)
     * @param newPaths paths of every data/delete file the commit added
     * @return TRUE if the commit is in the table, FALSE if it definitively is
     *         not, null if the history cannot be walked conclusively (the
     *         outcome stays unknown)
     */
    static Boolean commitLanded(Table fresh, Snapshot base, Set<String> newPaths) {
        Snapshot snapshot = fresh.currentSnapshot();
        while (snapshot != null) {
            if (base != null && snapshot.snapshotId() == base.snapshotId()) {
                return Boolean.FALSE;
            }
            for (DataFile file : snapshot.addedDataFiles(fresh.io())) {
                if (newPaths.contains(file.path().toString())) {
                    return Boolean.TRUE;
                }
            }
            for (DeleteFile file : snapshot.addedDeleteFiles(fresh.io())) {
                if (newPaths.contains(file.path().toString())) {
                    return Boolean.TRUE;
                }
            }
            Long parentId = snapshot.parentId();
            if (parentId == null) {
                /* reached the first snapshot without meeting the base */
                return base == null ? Boolean.FALSE : null;
            }
            snapshot = fresh.snapshot(parentId);
            /* parent expired out of the metadata: chain is inconclusive */
        }
        if (base == null) {
            /* the table was empty before and still has no head */
            return fresh.currentSnapshot() == null ? Boolean.FALSE : null;
        }
        return null;
    }

    private static final class GuardedOperations implements TableOperations {
        private final TableOperations delegate;

        GuardedOperations(TableOperations delegate) {
            this.delegate = delegate;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            try {
                delegate.commit(base, metadata);
            } catch (RESTException e) {
                /*
                 * Subclasses mean the server answered and ErrorHandlers
                 * already classified the answer; only the bare wrapper means
                 * "no response at all".
                 */
                if (e.getClass() != RESTException.class) {
                    throw e;
                }
                throw new CommitStateUnknownException(e);
            }
        }

        @Override
        public TableMetadata current() {
            return delegate.current();
        }

        @Override
        public TableMetadata refresh() {
            return delegate.refresh();
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption() {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }

        @Override
        public TableOperations temp(TableMetadata uncommittedMetadata) {
            return delegate.temp(uncommittedMetadata);
        }

        @Override
        public long newSnapshotId() {
            return delegate.newSnapshotId();
        }
    }
}
