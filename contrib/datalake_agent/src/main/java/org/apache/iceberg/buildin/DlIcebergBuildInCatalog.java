package org.apache.iceberg.buildin;

import cloud.elastic.dlagent.api.security.SecureLogin;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;

import java.util.Collections;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.buildin.DlIcebergBuildInTableOperations;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.util.LocationUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class DlIcebergBuildInCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {
    public static final String LIST_ALL_TABLES = "list-all-tables";
    public static final String LIST_ALL_TABLES_DEFAULT = "false";

    private static final Logger LOG = LoggerFactory.getLogger(DlIcebergBuildInCatalog.class);

    private String name;
    private Configuration conf;
    private FileIO fileIO;
    private ClientPool<IMetaStoreClient, TException> clients;
    private boolean listAllTables = false;
    private Map<String, String> catalogProperties;
    private final SecureLogin secureLogin;
    private final String serverName;
    private final String configFile;
    private Map<String, String> buildInCatalogProperties;

    public DlIcebergBuildInCatalog(SecureLogin secureLogin, String serverName, String configFile, Map<String, String> buildInCatalogProperties) {
        this.secureLogin = secureLogin;
        this.serverName = serverName;
        this.configFile = configFile;
        this.buildInCatalogProperties = ImmutableMap.copyOf(buildInCatalogProperties);
    }

    @Override
    public void initialize(String inputName, Map<String, String> properties) {
        this.catalogProperties = ImmutableMap.copyOf(properties);
        this.name = inputName;
        if (conf == null) {
            LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
            this.conf = new Configuration();
        }

        if (properties.containsKey(CatalogProperties.URI)) {
            this.conf.set(HiveConf.ConfVars.METASTOREURIS.varname, properties.get(CatalogProperties.URI));
        }

        if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
            this.conf.set(
                    HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                    LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION)));
        }

        this.listAllTables =
                Boolean.parseBoolean(properties.getOrDefault(LIST_ALL_TABLES, LIST_ALL_TABLES_DEFAULT));

        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        this.fileIO =
                fileIOImpl == null
                        ? new HadoopFileIO(conf)
                        : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        throw new RuntimeException("nosupport listTables");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!isValidIdentifier(identifier)) {
            throw new NoSuchTableException("Invalid identifier: %s", identifier);
        }
        String database = identifier.namespace().level(0);

        TableOperations ops = newTableOps(identifier);
        TableMetadata lastMetadata = null;
        if (purge) {
            try {
              lastMetadata = ops.current();
            } catch (NotFoundException e) {
              LOG.warn(
                  "Failed to load table metadata for table: {}, continuing drop without purge",
                  identifier,
                  e);
            }
        }

        try {
            if (purge && lastMetadata != null) {
              CatalogUtil.dropTableData(ops.io(), lastMetadata);
            }
            // HadoopCatalog is path-based: a table is represented by its directory under the warehouse.
            // There is no external metastore entry to remove, so dropping a table must delete that directory.
            // The 'purge' flag only controls whether to also delete data that may live outside the table dir
            // (e.g., when data/metadata are stored in different locations). but we is builtin(internal) catalog,
            // so no need to drop path

            LOG.info("Dropped table: {}", identifier);
            return true;

          } catch (NoSuchTableException e) {
            LOG.info("Skipping drop, table does not exist: {}", identifier, e);
            return false;

          }
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
        throw new RuntimeException("nosupport renameTable");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        throw new RuntimeException("nosupport createNamespace");
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        throw new RuntimeException("nosupport listNamespaces");
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        throw new RuntimeException("nosupport dropNamespace");
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        throw new RuntimeException("nosupport setProperties");
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        throw new RuntimeException("nosupport removeProperties");
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        throw new RuntimeException("nosupport loadNamespaceMetadata");
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    /**
     * The catalog's FileIO, without loading any table.
     *
     * <p>newTableOps(id).io() returns exactly this object, but constructing TableOperations
     * only to read io() invites someone to call refresh() on it -- which costs one
     * metadata.json GET. Callers that need storage access but not table state (the
     * loadMetadataJson endpoint) should use this instead.
     */
    public FileIO io() {
        return fileIO;
    }

    @Override
    public TableOperations newTableOps(TableIdentifier tableIdentifier) {
      String dbName = tableIdentifier.namespace().level(0);
      String tableName = tableIdentifier.name();
      return new DlIcebergBuildInTableOperations(conf, fileIO, name, dbName, tableName, buildInCatalogProperties);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        throw new RuntimeException("nosupport defaultWarehouseLocation");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("uri", this.conf == null ? "" : this.conf.get(HiveConf.ConfVars.METASTOREURIS.varname))
            .toString();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    protected Map<String, String> properties() {
        return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
    }
}
