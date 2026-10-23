/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.api.model.IcebergRequestConfigParser;
import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.api.model.iceberg.IcebergRequestConfig;
import cloud.elastic.dlagent.api.utilities.ColumnDescriptor;
import cloud.elastic.dlagent.constants.IcebergConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cloud.elastic.dlagent.service.iceberg.IcebergConfigurationBuilder.redactSecrets;

/**
 * Builds the {@link RequestContext} that every Iceberg operation starts from,
 * and extracts the property maps a request carries.
 *
 * <p>This is the real seam between a transport and the business logic: every
 * table-level operation in {@link IcebergOperations} begins by calling
 * {@code createRequestContext(namespace, table, properties)}, whichever
 * transport the request arrived on. The bodies are those the REST controller
 * always ran, unchanged.
 *
 * <p>Depends on {@link IcebergConfigurationBuilder} because a RequestContext
 * carries the resolved Hadoop {@link Configuration}; that dependency direction
 * (context factory -> configuration builder) is the one the controller already
 * had internally.
 *
 * <p>See the note in {@code MetadataFileCleaner} on why both {@code log} and
 * {@code LOG} exist here.
 */
@Named
@Singleton
@Slf4j
public class IcebergRequestContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRequestContextFactory.class);

    private final IcebergRequestConfigParser requestParser;
    private final IcebergConfigurationBuilder configurationBuilder;

    @Inject
    public IcebergRequestContextFactory(IcebergRequestConfigParser requestParser,
                                        IcebergConfigurationBuilder configurationBuilder) {
        this.requestParser = requestParser;
        this.configurationBuilder = configurationBuilder;
    }

    public Map<String, String> extractProperties(Map<String, Object> request) {
        IcebergRequestConfig cfg = requestParser.parse(request);
        Map<String, String> properties = cfg.toFlatPropertiesMap();

        // Preserve legacy: pass-through the raw request.properties unfiltered
        // so downstream code that reads buildInCatalog.* / other internal keys
        // continues to work.
        Object rawProps = request.get(IcebergConfigConstants.PROPERTIES);
        if (rawProps instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> userProps = (Map<String, Object>) rawProps;
            for (Map.Entry<String, Object> entry : userProps.entrySet()) {
                if (entry.getValue() != null) {
                    properties.put(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Extracted properties: {}", redactSecrets(properties));
        }
        return properties;
    }
    public Map<String, String> extractUserTableProperties(Map<String, Object> request) {
        return requestParser.parseUserTableProperties(request);
    }
    public void populateFilterTupleDescription(Map<String, Object> request, RequestContext context) {
        Object cfg = request.get("IcebergConfig");
        if (!(cfg instanceof Map)) {
            return;
        }
        Object add = ((Map<String, Object>) cfg).get("IcebergAdditionalConfig");
        if (!(add instanceof Map)) {
            return;
        }
        Object cols = ((Map<String, Object>) add).get("filterColumns");
        if (!(cols instanceof List)) {
            return;
        }

        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        tupleDescription.clear();

        int index = 0;
        for (Object o : (List<Object>) cols) {
            // Keep list position aligned with the filter's 0-based column index
            // (varattno-1): a non-Map or a name-less placeholder (e.g. a dropped
            // PG attribute) becomes a null slot rather than shrinking the list,
            // which would misalign every following attribute.
            if (!(o instanceof Map)) {
                tupleDescription.add(null);
                index++;
                continue;
            }
            Map<String, Object> col = (Map<String, Object>) o;
            String name = (String) col.get("name");
            if (name == null) {
                tupleDescription.add(null);
                index++;
                continue;
            }
            int oid = col.get("oid") == null ? 0 : ((Number) col.get("oid")).intValue();
            Object typmodObj = col.get("typmod");
            Integer[] typeMods = null;
            if (typmodObj instanceof Number) {
                int typmod = ((Number) typmodObj).intValue();
                if (typmod >= 0) {
                    typeMods = new Integer[] { typmod };
                }
            }
            tupleDescription.add(new ColumnDescriptor(name, oid, index++, null, typeMods));
        }
    }
    public RequestContext createRequestContext(String namespace, String table, Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            // Logging only keys to prevent sensitive data to be logged
            LOG.debug("Parsing request parameters: {}", String.join(", ", properties.keySet()));
        }
        // Set the properties in the context
        RequestContext context = new RequestContext();

        String totalSegmentKey = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.TOTAL_SEGMENT;
        String splitSizeKey = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.SPLIT_SIZE;
        String filterStringKey = IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.ICEBERG_ADDITIONAL_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_ADDITIONAL_CONFIG.FILTER_STRING;

        int totalSegments = Integer.parseInt(properties.getOrDefault(totalSegmentKey, "1"));
        String splitSize = properties.getOrDefault(splitSizeKey, "128");
        String filterString = properties.getOrDefault(filterStringKey, "");
        context.setTotalSegments(totalSegments);
        context.setSplitSize(splitSize);
        if (!filterString.isEmpty()) {
            context.setFilterString(filterString);
        }

        String usernameKey = IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.ICEBERG_VOLUME_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_VOLUME_CONFIG.USERNAME;
        String username = properties.getOrDefault(usernameKey, "");
        context.setUser(username);

        // Set common properties
        context.setSchemaName(namespace);
        context.setTableName(table);

        //in hive dataSource is schemaName + tableName
        String catalogServerTypeKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + 
                                    IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE;
        String catalogServerType = properties.getOrDefault(catalogServerTypeKey, "");

        if (catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_HIVE)
                || catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_S3)
                || catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_HADOOP)) {
            // For path-based and Hive catalogs the agent identifies tables by
            // <namespace>.<table>. RequestContext.getDataSource() falls back to
            // RequestContext.path when dataSource is null, which would incorrectly
            // hand the warehouse URL to IcebergUtilities.getIcebergTableIdentifier
            // and produce "default.<warehouse-url>" as the table identifier.
            String dataSource = namespace + "." + table;
            context.setDataSource(dataSource);
        } else if (catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_POLARIS)
                || catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_REST)) {
            String catalogNameKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + 
            IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.CATALOG_NAME;
            String catalogName = properties.getOrDefault(catalogNameKey, "");
            if (catalogName.isEmpty()) {
                String label = catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_POLARIS)
                        ? "polaris" : "REST";
                throw new IllegalStateException("iceberg " + label
                        + " catalog catalog_name is empty, must specify catalog_name.");
            }
            if (namespace.isEmpty()) {
                String label = catalogServerType.equals(IcebergConfigConstants.CATALOG_TYPE_POLARIS)
                        ? "polaris" : "REST";
                throw new IllegalStateException("iceberg " + label
                        + " catalog namespace is empty, must specify namespace.");
            }
            String dataSource = catalogName + "." + namespace + "." + table;
            context.setDataSource(dataSource);
        }

        String serverTypeKey = IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.ICEBERG_CATALOG_CONFIG_STRING + "." + IcebergConfigConstants.ICEBERG_CATALOG_CONFIG.SERVER_TYPE;
        String server_type = properties.getOrDefault(serverTypeKey, "");
        if (server_type.isEmpty()) {
            throw new IllegalStateException("iceberg catalog server_type is empty, must specify server_type.");
        }
        context.setCatalogType(server_type);

        // Set Gopher properties
        Map<String, String> gopherProps = configurationBuilder.getGopherProps(properties);
        context.setGopherProperties(gopherProps);
        // Diagnostic INFO line: gopher params plumbed to catalog (secrets redacted).
        // Helps spot misrouted params like endpoint with scheme leftover, ufs_type=s3
        // (gopher rejects), missing useVirtualHost, etc. that previously caused
        // CURLE_COULDNT_RESOLVE_HOST / "unsupported UFS type: s3".
        if (LOG.isInfoEnabled() && !gopherProps.isEmpty()) {
            LOG.info("gopher props -> catalog: {}", redactSecrets(gopherProps));
            /* Diagnostic trace: see iceberg_volume_option.c step2 for context. */
            if (LOG.isDebugEnabled()) {
                String ak = gopherProps.get("gopher.access_key");
                String sk = gopherProps.get("gopher.secret_key");
                LOG.debug("[trace_ak] step6 gopherProps->catalog: gopher.access_key len={}, gopher.secret_key len={}",
                    ak == null ? -1 : ak.length(),
                    sk == null ? -1 : sk.length());
            }
        }

        // Set context configuration
        Configuration configuration = configurationBuilder.getConfiguration(properties);
        context.setConfiguration(configuration);

        // Get warehouse location and set it as path.
        //
        // For most catalog types this is the full URL.  For type='hadoop' the
        // path is consumed by IcebergHadoopCatalog as `catalogLocation` in the
        // formula `WAREHOUSE_LOCATION = fs.defaultFS + catalogLocation`, so we
        // store only the path component here (with leading '/').  fs.defaultFS
        // for hadoop catalog is set in setCatalogWarehouseInline.
        String warehouseLocation = configurationBuilder.getCatalogWarehouseLocationPrefix(properties);
        if (server_type.equals(IcebergConfigConstants.CATALOG_TYPE_HADOOP)
                && warehouseLocation != null && !warehouseLocation.isEmpty()) {
            int schemeEnd = warehouseLocation.indexOf("://");
            if (schemeEnd >= 0) {
                int pathStart = warehouseLocation.indexOf('/', schemeEnd + 3);
                warehouseLocation = (pathStart >= 0)
                    ? warehouseLocation.substring(pathStart)
                    : "";
            }
        }
        context.setPath(warehouseLocation);

        if (server_type.equalsIgnoreCase(IcebergConfigConstants.CATALOG_TYPE_BUILDIN)) {
            Map<String, String> buildInCatalogProps = configurationBuilder.getBuildInCatalogProps(properties);
            context.setBuildInCatalogProperties(buildInCatalogProps);
        }

        return context;
    }
}
