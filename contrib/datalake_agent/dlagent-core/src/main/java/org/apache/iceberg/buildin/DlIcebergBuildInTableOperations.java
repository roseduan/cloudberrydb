package org.apache.iceberg.buildin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;

public class DlIcebergBuildInTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(DlIcebergBuildInTableOperations.class);
  private static final String TABLE_EXISTS_PROP = "buildInCatalog.table_exists";
  private static final String METADATA_LOCATION_PROP = "buildInCatalog.metadata_location";

  private final String fullName;
  private final FileIO fileIO;
  private final Map<String, String> tableProperties;
  private final int metadataRefreshMaxRetries;

  public DlIcebergBuildInTableOperations(
      Configuration conf,
      FileIO fileIO,
      String catalogName,
      String database,
      String table,
      Map<String, String> tableProperties) {
    this.fullName = catalogName + "." + database + "." + table;
    this.fileIO = fileIO;
    this.tableProperties = tableProperties != null ? new HashMap<>(tableProperties) : Collections.emptyMap();
    this.metadataRefreshMaxRetries = conf.getInt("iceberg.metadata-refresh-max-retries", 2);
  }

  @Override
  protected String tableName() {
    return fullName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    String key = IcebergConfigConstants.BUILDIN_CATALOG_OPTION.BUILDIN_CATALOG_STRING + "." 
      + IcebergConfigConstants.BUILDIN_CATALOG_OPTION.TABLE_EXISTS_PROP;
    boolean tableExists = Boolean.parseBoolean(tableProperties.get(key));

    if (tableExists) {
      key = IcebergConfigConstants.BUILDIN_CATALOG_OPTION.BUILDIN_CATALOG_STRING + "." 
      + IcebergConfigConstants.BUILDIN_CATALOG_OPTION.METADATA_LOCATION_PROP;
      metadataLocation = tableProperties.get(key);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("No such table: %s", fullName);
      }
    }

    refreshFromMetadataLocation(metadataLocation, metadataRefreshMaxRetries);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    // set table exists key & value
    String key = IcebergConfigConstants.BUILDIN_CATALOG_OPTION.BUILDIN_CATALOG_STRING + "." 
      + IcebergConfigConstants.BUILDIN_CATALOG_OPTION.TABLE_EXISTS_PROP;
    tableProperties.put(key, "true");
    // set newMetadataLocation key & value
    key = IcebergConfigConstants.BUILDIN_CATALOG_OPTION.BUILDIN_CATALOG_STRING + "." 
    + IcebergConfigConstants.BUILDIN_CATALOG_OPTION.METADATA_LOCATION_PROP;
    tableProperties.put(key, newMetadataLocation);

    LOG.info(
        "Committed to table {} with the new metadata location {}", fullName, newMetadataLocation);
  }
}
