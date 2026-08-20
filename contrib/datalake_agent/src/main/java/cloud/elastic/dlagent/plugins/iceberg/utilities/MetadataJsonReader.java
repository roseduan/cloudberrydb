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

package cloud.elastic.dlagent.plugins.iceberg.utilities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

/**
 * Reads a table's metadata.json as raw bytes.
 *
 * <p>Lives here rather than on IcebergRestController so the service layer can use it without
 * depending on the REST layer.
 */
public final class MetadataJsonReader {

    private MetadataJsonReader() {}

    /**
     * Read a metadata.json verbatim. Deliberately NOT TableMetadataParser.read + toJson: dlagent
     * runs iceberg 1.3.0 while the REST catalog gateway runs 1.6.1, so a round trip through this
     * JVM's TableMetadata model would silently drop any field 1.3.0 does not model. Byte
     * passthrough is version-agnostic.
     */
    public static byte[] readMetadataBytes(FileIO io, String metadataLocation) throws IOException {
        if (metadataLocation == null || metadataLocation.trim().isEmpty()) {
            throw new IllegalArgumentException("metadataLocation is required");
        }
        try (InputStream in = io.newInputFile(metadataLocation).newStream();
             ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            byte[] chunk = new byte[8192];
            int n;
            while ((n = in.read(chunk)) != -1) {
                buf.write(chunk, 0, n);
            }
            return buf.toByteArray();
        }
    }

    /**
     * A table whose current metadata pointer is null/blank is, for the loadMetadataJson
     * endpoint's purposes, indistinguishable from a table that does not exist -- the caller has
     * nothing to read. Surfaced as NoSuchTableException (not IllegalArgumentException) so it maps
     * to the same 404 + ErrorModel shape as an actual missing table, per spec.
     */
    public static void requireMetadataLocationPresent(String metadataLocation, String namespace,
            String table) {
        if (metadataLocation == null || metadataLocation.trim().isEmpty()) {
            throw new NoSuchTableException("No metadata location for table: %s.%s", namespace, table);
        }
    }
}
