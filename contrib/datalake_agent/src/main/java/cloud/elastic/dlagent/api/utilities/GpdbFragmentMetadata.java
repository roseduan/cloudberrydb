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

package cloud.elastic.dlagent.api.utilities;

import java.util.List;

public class GpdbFragmentMetadata implements FragmentMetadata {
	public static enum ContentType {
        DATA_FILE,
        POSITION_DELETE,
        EQUALITY_DELETE
    }
	private long rowCount;
	private long fileSize;
	private String fileFormat;
	private ContentType contentType;
	/*
	 * Iceberg partition values (identity, spec order) for this data file, as
	 * sent by the segment writer.  A null element means SQL NULL; null/empty
	 * list means unpartitioned.
	 */
	private List<String> partitionValues;

	public GpdbFragmentMetadata(long fileSize, String fileFormat, long rowCount) {
		this.rowCount = rowCount;
		this.fileSize = fileSize;
		this.fileFormat = fileFormat;
		this.contentType = ContentType.DATA_FILE;
	}

	public GpdbFragmentMetadata(long fileSize, String fileFormat, long rowCount, ContentType type) {
		this.rowCount = rowCount;
		this.fileSize = fileSize;
		this.fileFormat = fileFormat;
		this.contentType = type;
	}

	public GpdbFragmentMetadata(long fileSize, String fileFormat, long rowCount, String type) {
		this.rowCount = rowCount;
		this.fileSize = fileSize;
		this.fileFormat = fileFormat;
		this.contentType = ContentType.valueOf(type);
	}

	public long getRowCount() {
		return rowCount;
	}

	public long getFileSize() {
		return fileSize;
	}

	public String getFileFormat() {
		return fileFormat;
	}

	public ContentType getContentType() {
		return contentType;
	}

	public List<String> getPartitionValues() {
		return partitionValues;
	}

	public void setPartitionValues(List<String> partitionValues) {
		this.partitionValues = partitionValues;
	}

}
