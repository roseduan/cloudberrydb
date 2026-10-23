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

package cloud.elastic.dlagent.plugins.hudi;

import cloud.elastic.dlagent.api.error.UnsupportedTypeException;
import cloud.elastic.dlagent.api.filter.Operator;
import cloud.elastic.dlagent.api.io.DataType;
import cloud.elastic.dlagent.api.model.BasePlugin;
import cloud.elastic.dlagent.api.model.MetadataFetcher;
import cloud.elastic.dlagent.api.model.ScanTask;
import cloud.elastic.dlagent.api.model.CombinedTask;
import cloud.elastic.dlagent.api.model.Partition;
import cloud.elastic.dlagent.api.model.Metadata;
import cloud.elastic.dlagent.api.model.Fragment;
import cloud.elastic.dlagent.api.model.FragmentDescription;
import cloud.elastic.dlagent.api.utilities.BeanLocator;
import cloud.elastic.dlagent.api.utilities.Utilities;
import cloud.elastic.dlagent.plugins.hudi.utilities.HudiUtilities;
import com.google.common.collect.Lists;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.common.util.collection.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class HudiMetadataFetcher extends BasePlugin implements MetadataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(HudiMetadataFetcher.class);

    // ----- members for predicate pushdown handling -----
    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.NOOP,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    //Operator.LIKE,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.OR,
                    Operator.AND,
                    Operator.NOT
            );

    static final EnumSet<DataType> SUPPORTED_DATATYPES =
            EnumSet.of(
                    DataType.BIGINT,
                    DataType.INTEGER,
                    DataType.SMALLINT,
                    DataType.REAL,
                    DataType.NUMERIC,
                    DataType.FLOAT8,
                    DataType.TEXT,
                    DataType.VARCHAR,
                    DataType.BPCHAR,
                    DataType.BOOLEAN,
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.TIMESTAMP_WITH_TIME_ZONE,
                    DataType.TIME,
                    DataType.BYTEA
            );

    private final HudiCatalogWrapper hudiClientWrapper;
    protected final HudiUtilities hudiUtilities;

    public HudiMetadataFetcher() {
        this(BeanLocator.getBean(HudiUtilities.class), BeanLocator.getBean(HudiCatalogWrapper.class));
    }

    HudiMetadataFetcher(HudiUtilities hudiUtilities, HudiCatalogWrapper hudiClientWrapper) {
        this.hudiUtilities = hudiUtilities;
        this.hudiClientWrapper = hudiClientWrapper;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
    }

    public FragmentDescription getFragments(String pattern) throws Exception {
        Metadata.Item tableName = Utilities.extractTableFromName(context.getDataSource());
        LOG.info("Get fragments for table {}", tableName);
        HudiCatalog catalog = hudiClientWrapper.getHudiCatalog(tableName, context);

        hudiUtilities.verifySchema(catalog.getSchema(tableName), context);

        return transformTasks(catalog.getSplits(tableName, context));
    }

    public List<Partition> getPartitions(String pattern) throws Exception {
        throw new UnsupportedOperationException("Hudi accessor does not support getPartitions operation.");
    }

    public Metadata getSchema(String pattern) throws Exception {
        Metadata.Item tableName = Utilities.extractTableFromName(context.getDataSource());
        Metadata metadata = new Metadata(tableName);

        HudiCatalog catalog = hudiClientWrapper.getHudiCatalog(tableName, context);

        try {
            for (Types.Field hudiCol : catalog.getSchema(tableName).columns()) {
                metadata.addField(hudiUtilities.mapHudiType(hudiCol));
            }
        } catch (UnsupportedTypeException e) {
            String errorMsg = "Failed to retrieve metadata for table " + metadata.getItem() + ". " +
                    e.getMessage();
            throw new UnsupportedTypeException(errorMsg);
        }
        return metadata;
    }

    private FragmentDescription transformTasks(Pair<HudiTableOptions, List<CombineHudiSplit>> pair) {
        List<CombinedTask> tasks = Lists.newArrayList();

        for (CombineHudiSplit combinedHudiSplit : pair.getRight()) {
            List<ScanTask> combinedTask = Lists.newArrayList();
            for (HudiSplit hudiSplit : combinedHudiSplit.getCombineSplit()) {
                HudiFile baseFile = hudiSplit.getBaseFile().orElse(null);
                Fragment data = null;
                if (baseFile != null) {
                    data = new Fragment(baseFile.getPath(),
                            new HudiFileFragmentMetadata(convertFileFormat(baseFile.getFileFormat()),
                                    "DATA", baseFile.getLength()));
                }

                List<Fragment> logFiles = Lists.newArrayList();
                for (HudiFile logFile : hudiSplit.getLogFiles()) {
                    Fragment log = new Fragment(logFile.getPath(),
                            new HudiFileFragmentMetadata(convertFileFormat(logFile.getFileFormat()),
                                    "DELTA_LOG", logFile.getLength()));
                    logFiles.add(log);
                }

                combinedTask.add(new ScanTask(data, logFiles,
                        baseFile == null ? 0 : baseFile.getStart(),
                        baseFile == null ? 0: baseFile.getLength(),
                        hudiSplit.getInstantTime()));
            }
            tasks.add(new CombinedTask(combinedTask));
        }

        return new FragmentDescription(pair.getLeft(), tasks);
    }

    private String convertFileFormat(HoodieFileFormat fileFormat) {
        switch (fileFormat) {
            case PARQUET:
                return "PARQUET";
            case ORC:
                return "ORC";
            case HFILE:
                return "HFILE";
            default:
                return "HLOG";
        }
    }

    static public EnumSet<DataType> getSupportedDatatypesForPushdown() {
        return SUPPORTED_DATATYPES;
    }

    static public EnumSet<Operator> getSupportedOperatorsForPushdown() {
        return SUPPORTED_OPERATORS;
    }

    public boolean open() throws Exception {
        return true;
    }

    public void close() throws Exception {
    }

    @Override
    public Boolean batchAppend() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'batchAppend'");
    }

    @Override
    public Metadata getOrCreateSchema() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'getSchemaOrCreate'");
    }

    @Override
    public Boolean rowUpdate() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'rowUpdate'");
    }

    @Override
    public Map<String, String> getCurrentSnapshotSummary() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'getCurrentSnapshotSummary'");
    }
}
