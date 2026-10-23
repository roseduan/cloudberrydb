/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.service.iceberg.IcebergOperations;

import java.util.List;
import java.util.Map;

/**
 * An {@link IcebergOperations} that returns a fixed result and records the
 * routing values it was called with.
 *
 * <p>{@link GrpcTransportEquivalenceTest} is about the transport shell --
 * marshalling, status mapping, element boundaries, metadata routing -- so the
 * operations underneath are stubbed. Using the real ones would need a catalog
 * and object storage, which is an end-to-end test's job, and would let an Iceberg failure
 * masquerade as a transport bug.
 */
class StubOperations extends IcebergOperations {

    private final ServiceResult<Object> result;
    private final List<String> routingSeen;

    StubOperations(ServiceResult<Object> result, List<String> routingSeen) {
        super(null, null, null, null, null);
        this.result = result;
        this.routingSeen = routingSeen;
    }

    private ServiceResult<Object> record(String prefix, String table) {
        routingSeen.add(prefix);
        routingSeen.add(table);
        return result;
    }

    @Override
    public ServiceResult<Object> tableExists(String prefix, String table, Map<String, Object> request) {
        return record(prefix, table);
    }

    @Override
    public ServiceResult<Object> loadTable(String prefix, String table, Map<String, Object> request,
                                           String ifNoneMatch) {
        return record(prefix, table);
    }

    @Override
    public ServiceResult<Object> planFileGroups(String prefix, String table, Map<String, Object> request) {
        return record(prefix, table);
    }
}
