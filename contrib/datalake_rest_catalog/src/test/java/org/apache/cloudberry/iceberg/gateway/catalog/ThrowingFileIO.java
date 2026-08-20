package org.apache.cloudberry.iceberg.gateway.catalog;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * A FileIO that fails loudly on any actual use. Injected into the read path to PROVE the
 * gateway performs no object storage IO -- an assertion no amount of reading the code can
 * make, because iceberg-core could lazily reach for io() anywhere in LoadTableResult
 * serialization. If a test using this class passes, that path really is IO-free.
 */
public final class ThrowingFileIO implements FileIO {

    public static final class UnexpectedIoException extends RuntimeException {
        UnexpectedIoException(String op, String path) {
            super("gateway performed object storage IO it should not: " + op + " " + path);
        }
    }

    @Override
    public InputFile newInputFile(String path) {
        throw new UnexpectedIoException("newInputFile", path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnexpectedIoException("newOutputFile", path);
    }

    @Override
    public void deleteFile(String path) {
        throw new UnexpectedIoException("deleteFile", path);
    }
}
