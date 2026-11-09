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

import lombok.Getter;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class FragmentDescription {
    private TableOptions tableOptions;
    private List<Fragment> deleteFiles;
    private List<CombinedTask> combinedTasks;
    private Long snapshotId;
    private boolean notModified;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("catalog-properties")
    private Map<String, Object> catalogProperties;
    /*
     * The read schema's top-level columns as name -> Iceberg field-id.
     *
     * The reader matches a data file's columns by field-id, which is what
     * makes a column readable after a RENAME: the file keeps the old name and
     * the id, the table keeps the id and the new name, and only the id joins
     * them.  Without this the segment has nothing but names to go on and a
     * renamed column reads NULL out of every file written before the rename
     * (#482).
     *
     * Omitted when empty, so a segment that does not know the key behaves as
     * it did before and an older agent is read by a newer segment.
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("schema-field-ids")
    private Map<String, Integer> schemaFieldIds;

    public FragmentDescription(TableOptions tableOptions, List<CombinedTask> combinedTasks) {
        this.tableOptions = tableOptions;
        this.combinedTasks = combinedTasks;
    }

    public FragmentDescription(TableOptions tableOptions, List<CombinedTask> combinedTasks, Long snapshotId) {
        this.tableOptions = tableOptions;
        this.combinedTasks = combinedTasks;
        this.snapshotId = snapshotId;
    }

    public FragmentDescription(TableOptions tableOptions, List<Fragment> deleteFiles,
                               List<CombinedTask> combinedTasks, Long snapshotId) {
        this.tableOptions = tableOptions;
        this.deleteFiles = deleteFiles;
        this.combinedTasks = combinedTasks;
        this.snapshotId = snapshotId;
    }

    public static FragmentDescription notModified(long snapshotId) {
        FragmentDescription desc = new FragmentDescription(null, null, snapshotId);
        desc.notModified = true;
        return desc;
    }
}
