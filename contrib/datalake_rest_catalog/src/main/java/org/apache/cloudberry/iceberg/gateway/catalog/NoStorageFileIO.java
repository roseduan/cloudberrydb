package org.apache.cloudberry.iceberg.gateway.catalog;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * The gateway holds no object storage credentials: metadata.json is fetched through the
 * database (pg_ext_aux.iceberg_load_metadata) and data files are read by the client
 * directly. TableOperations.io() must still return something, so return an implementation
 * that fails with a clear message rather than a half-configured S3 client.
 *
 * <p>NoObjectStorageIoTest drives the whole loadTable + serialization path with the test-side
 * twin of this class and asserts nothing throws, so reaching any method here means a genuinely
 * new code path started wanting storage access -- not that this class is under-implemented.
 */
public final class NoStorageFileIO implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
        throw new UnsupportedOperationException(
                "the REST catalog gateway does not read object storage; got: " + path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException("read-only catalog: " + path);
    }

    @Override
    public void deleteFile(String path) {
        throw new UnsupportedOperationException("read-only catalog: " + path);
    }
}
