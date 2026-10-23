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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides backwards compatibility for Hive 1.x.x servers.
 * In Hive 1.x.x, an API call get_table_req is made, however this API
 * call was introduced in Hive version 2. This class provides a fallback
 * for older Hive servers to still be able query metadata.
 * <p>
 * The motivation for this approach is taken from here:
 * https://github.com/HotelsDotCom/waggle-dance/pull/133/files
 */
@SuppressWarnings("deprecation")
public class HiveMetaStoreClientCompatibility1xx extends HiveMetaStoreClient implements IMetaStoreClient {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public HiveMetaStoreClientCompatibility1xx(Configuration conf) throws MetaException {
        super(conf);
    }

    public HiveMetaStoreClientCompatibility1xx(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
        super(conf, hookLoader);
    }

    public HiveMetaStoreClientCompatibility1xx(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    /**
     * Returns the table given a dbname and table name. This will fallback
     * in Hive 1.x.x servers when a get_table_req API call is made.
     *
     * @param dbname The database the table is located in.
     * @param name   Name of the table to fetch.
     * @return An object representing the table.
     * @throws TException A thrift communication error occurred
     */
    @Override
    public Table getTable(String dbname, String name) throws TException {
        try {
            return getTable(null, dbname, name);
        } catch (TException e) {
            try {
                LOG.debug("Couldn't invoke method getTable");
                if (e.getClass().isAssignableFrom(TApplicationException.class)) {
                    LOG.debug("Attempting to fallback");
                    Table table = client.get_table(dbname, name);
                    return new GetTableResult(table).getTable();
                }
            } catch (MetaException | NoSuchObjectException ex) {
                LOG.debug("Original exception not re-thrown", e);
                throw ex;
            } catch (TTransportException transportException) {
                /*
                Propagate a TTransportException to allow RetryingMetaStoreClient (which proxies this class) to retry connecting to the metastore.
                The number of retries can be set in the hive-site.xml using hive.metastore.failure.retries.
                 */
                throw transportException;
            } catch (Throwable t) {
                LOG.warn("Unable to run compatibility for metastore client method get_table_req. Will rethrow original exception: ", t);
            }
            throw e;
        }
    }
}
