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

package cloud.elastic.dlagent.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Request model for file list sent in POST request body.
 * This is used for write operations to avoid HTTP header size limits.
 */
public class FileListRequest {

    private List<FileEntry> files;

    /**
     * Get the list of files
     */
    public List<FileEntry> getFiles() {
        return files;
    }

    /**
     * Set the list of files
     */
    public void setFiles(List<FileEntry> files) {
        this.files = files;
    }

    /**
     * Represents a single file entry in the file list
     */
    public static class FileEntry {
        private String filePath;
        private String format;
        private String content;
        private Long fileSize;
        private Long recordCount;
        /*
         * Per-column statistics the segment read out of this file's own footer
         * at write time (wire format v1, issue #442).  Held as the parsed JSON
         * object and not interpreted here; null when the entry carried none,
         * which leaves the agent reading the footer itself.
         */
        private Object columnStats;

        /**
         * Get the file path
         */
        @JsonProperty("filePath")
        public String getFilePath() {
            return filePath;
        }

        /**
         * Set the file path
         */
        @JsonProperty("filePath")
        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        /**
         * Get the file format (ORC, PARQUET, AVRO, etc.)
         */
        @JsonProperty("format")
        public String getFormat() {
            return format;
        }

        /**
         * Set the file format
         */
        @JsonProperty("format")
        public void setFormat(String format) {
            this.format = format;
        }

        /**
         * Get the content type (DATA_FILE, POSITION_DELETE, etc.)
         */
        @JsonProperty("content")
        public String getContent() {
            return content;
        }

        /**
         * Set the content type
         */
        @JsonProperty("content")
        public void setContent(String content) {
            this.content = content;
        }

        /**
         * Get the file size in bytes
         */
        @JsonProperty("fileSize")
        public Long getFileSize() {
            return fileSize;
        }

        /**
         * Set the file size in bytes
         */
        @JsonProperty("fileSize")
        public void setFileSize(Long fileSize) {
            this.fileSize = fileSize;
        }

        /**
         * Get the record count
         */
        @JsonProperty("recordCount")
        public Long getRecordCount() {
            return recordCount;
        }

        /**
         * Set the record count
         */
        @JsonProperty("recordCount")
        public void setRecordCount(Long recordCount) {
            this.recordCount = recordCount;
        }

        /**
         * Get the write-time column statistics, or null if absent
         */
        @JsonProperty("column_stats")
        public Object getColumnStats() {
            return columnStats;
        }

        /**
         * Set the write-time column statistics
         */
        @JsonProperty("column_stats")
        public void setColumnStats(Object columnStats) {
            this.columnStats = columnStats;
        }

        @Override
        public String toString() {
            return "FileEntry{" +
                    "filePath='" + filePath + '\'' +
                    ", format='" + format + '\'' +
                    ", content='" + content + '\'' +
                    ", fileSize=" + fileSize +
                    ", recordCount=" + recordCount +
                    '}';
        }
    }
}