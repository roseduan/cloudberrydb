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

package cloud.elastic.dlagent.service.controller;

import lombok.Getter;
import lombok.Setter;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.service.MetricsReporter;

/**
 * Holds statistics about performed operation.
 */
public class OperationStats {
    @Getter
    private final RequestContext context;
    private final MetricsReporter metricsReporter;
    private final long reportFrequency;
    @Getter
    private long rpcCount = 0;
    @Getter
    @Setter
    private long lastReportedRpcCount = 0;
    @Getter
    @Setter
    private Operation operation;

    enum Operation {
        PARTITION_GET(MetricsReporter.DlAgentMetric.PARTITION_GET),
        METADATA_GET(MetricsReporter.DlAgentMetric.METADATA_GET),
        FRAGMENT_GET(MetricsReporter.DlAgentMetric.FRAGMENT_GET),
        BATCH_APPEND(MetricsReporter.DlAgentMetric.BATCH_APPEND),
        SCHEMA_GET_OR_CREATE(MetricsReporter.DlAgentMetric.SCHEMA_GET_OR_CREATE),
        ROW_UPDATE(MetricsReporter.DlAgentMetric.ROW_UPDATE),
        CURRENT_SNAPSHOT_SUMMARY_GET(MetricsReporter.DlAgentMetric.CURRENT_SNAPSHOT_SUMMARY_GET);

        private final MetricsReporter.DlAgentMetric rpcCountMetric;

        Operation(MetricsReporter.DlAgentMetric recordMetric) {
            this.rpcCountMetric = recordMetric;
        }

        public MetricsReporter.DlAgentMetric getMetric() {
            return rpcCountMetric;
        }
    }

    public OperationStats(MetricsReporter metricsReporter, RequestContext context) {
        this.context = context;
        this.metricsReporter = metricsReporter;
        this.reportFrequency = metricsReporter.getReportFrequency();
    }

    /**
     * Add a completed record to the operation's stats. Report the stats when necessary.
     *
     */
    public void reportCompletedRpcCount() {
        rpcCount++;

        if ((reportFrequency != 0) && (rpcCount % reportFrequency == 0)) {
            flushStats();
        }
    }

    /**
     * Send all the stats to the metric reporter. Set last reported values.
     */
    public void flushStats() {
        if (reportFrequency == 0) {
            return;
        }

        long rpcProcessed = rpcCount - lastReportedRpcCount;
        if (rpcProcessed != 0) {
            metricsReporter.reportCounter(operation.rpcCountMetric, rpcProcessed, context);
            lastReportedRpcCount = rpcCount;
        }
    }
}
