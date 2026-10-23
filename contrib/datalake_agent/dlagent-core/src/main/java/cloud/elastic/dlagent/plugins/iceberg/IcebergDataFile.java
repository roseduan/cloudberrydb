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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;

public class IcebergDataFile implements DataFile{
	private String path;
	private Long fileSizeInBytes;
	private Long recordCount;
	private FileFormat format;

	public IcebergDataFile(String path, Long fileSizeInBytes, Long recordCount, FileFormat format) {
		this.path = path;
		this.fileSizeInBytes = fileSizeInBytes;
		this.recordCount = recordCount;
		this.format = format;
	}

	@Override
	public Long pos() {
		return null;
	}

	@Override
	public int specId() {
		return 0;
	}

	@Override
	public CharSequence path() {
		return path;
	}

	@Override
	public FileFormat format() {
		return format;
	}

	@Override
	public StructLike partition() {
		return null;
	}

	@Override
	public long recordCount() {
		return recordCount;
	}

	@Override
	public long fileSizeInBytes() {
		return fileSizeInBytes;
	}

	@Override
	public Map<Integer, Long> columnSizes() {
		return null;
	}

	@Override
	public Map<Integer, Long> valueCounts() {
		return null;
	}

	@Override
	public Map<Integer, Long> nullValueCounts() {
		return null;
	}

	@Override
	public Map<Integer, Long> nanValueCounts() {
		return null;
	}

	@Override
	public Map<Integer, ByteBuffer> lowerBounds() {
		return null;
	}

	@Override
	public Map<Integer, ByteBuffer> upperBounds() {
		return null;
	}

	@Override
	public ByteBuffer keyMetadata() {
		return null;
	}

	@Override
	public List<Long> splitOffsets() {
		return null;
	}

	@Override
	public DataFile copy() {
		return null;
	}

	@Override
	public DataFile copyWithoutStats() {
		return null;
	}
	
}
