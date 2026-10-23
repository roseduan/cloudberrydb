package cloud.elastic.dlagent.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.HttpClientErrorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test for IcebergBuildInCatalog using REST API calls
 * Tests the BuildInCatalog implementation specifically
 * 
 * To run this test:
 * 1. Start the application with JVM debug: java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar app.jar
 * 2. Run this test class
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class IcebergBuildInCatalogIntegrationTest {

    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;
    private String baseUrl;
    private String prefix;
    private String namespace = "default";
    private static final String newTableName = "buildin_test_table_" + System.currentTimeMillis();
    private static final String metadataLocation = null;

    @BeforeEach
    public void setUp() {
        /*
         * These post to a real agent. Skip rather than fail where none is up:
         * the routing and contract they used to pretend to cover now live in
         * IcebergRestControllerWebTest, which needs nothing running.
         */
        AgentAvailability.assumeAgentRunning();
        AgentAvailability.assumeStorageRunning();
        restTemplate = new RestTemplate();
        objectMapper = new ObjectMapper();
        // Adjust port if your application runs on different port
        baseUrl = "http://localhost:8080/api/v1";
        prefix = "";
    }

    private String format_metadata_location() {
        String metadata_location = "";
        if (metadataLocation == null) {
            metadata_location = "s3a://warehouse/hive_location/default.db/" + "buildin_test_table_1762240421261" + "/metadata/00000-5736f196-f087-4df1-ba52-b8ca30912c98.metadata.json";
        } else {
            metadata_location = metadataLocation;
        }
        return metadata_location;
    }

    @Test
    public void test1_TableExists_TableNotFound() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/exists", baseUrl, prefix, newTableName);

        Map<String, Object> request = createBuildInCatalogRequestConfig("false", "");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            fail("Expected HttpClientErrorException to be thrown");
        } catch (HttpClientErrorException e) {
            assertEquals(HttpStatus.NOT_FOUND, e.getStatusCode());
            assertNotNull(e.getResponseBodyAsString());
            
            System.out.println("BuildInCatalog - Table not found as expected: " + e.getResponseBodyAsString());
        }
    }

    @Test
    public void test2_CreateTable() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/create", baseUrl, prefix);

        Map<String, Object> request = createBuildInCatalogRequestConfig("false", "");
        request.put("name", newTableName);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("BuildInCatalog - Table created successfully");
                assertNotNull(response.getBody());
                
                Map<String, Object> responseBody = objectMapper.readValue(response.getBody(), Map.class);
                assertTrue(responseBody.containsKey("metadata"));
                assertTrue(responseBody.containsKey("metadata-location"));
                
                // Verify BuildInCatalog specific properties
                Map<String, Object> metadata = (Map<String, Object>) responseBody.get("metadata");
                assertNotNull(metadata.get("location"));
                
                System.out.println("BuildInCatalog - Table Location: " + metadata.get("location"));
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT) {
                System.out.println("BuildInCatalog - Table already exists");
            } else {
                System.out.println("BuildInCatalog - Error creating table: " + e.getStatusCode());
                System.out.println("Response: " + e.getResponseBodyAsString());
            }
        }
    }

    @Test
    public void test3_TableExists_TableFound() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/exists", baseUrl, prefix, newTableName);

        Map<String, Object> request = createBuildInCatalogRequestConfig("true", format_metadata_location());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("BuildInCatalog - Table exists confirmed");
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("BuildInCatalog - Table not found: " + e.getResponseBodyAsString());
            } else {
                System.out.println("BuildInCatalog - Unexpected error: " + e.getStatusCode());
                System.out.println("Response body: " + e.getResponseBodyAsString());
            }
        }
    }

    @Test
    public void test4_LoadTable() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/load", baseUrl, prefix, newTableName);

        Map<String, Object> request = createBuildInCatalogRequestConfig("true", format_metadata_location());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("BuildInCatalog - Table loaded successfully");
                assertNotNull(response.getBody());
                
                Map<String, Object> responseBody = objectMapper.readValue(response.getBody(), Map.class);
                assertTrue(responseBody.containsKey("metadata"));
                assertTrue(responseBody.containsKey("metadata-location"));
                
                // Verify BuildInCatalog specific metadata
                Map<String, Object> metadata = (Map<String, Object>) responseBody.get("metadata");
                assertNotNull(metadata.get("current-schema-id"));
                assertNotNull(metadata.get("schemas"));
                
                System.out.println("BuildInCatalog - Current Schema ID: " + metadata.get("current-schema-id"));
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("BuildInCatalog - Table not found for loading");
            } else {
                System.out.println("BuildInCatalog - Error loading table: " + e.getStatusCode());
                System.out.println("Response: " + e.getResponseBodyAsString());
            }
        }
    }

    @Test
    public void test5_GetFragment() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/getFragment", baseUrl, prefix, newTableName);

        Map<String, Object> request = createBuildInCatalogRequestConfig("true", format_metadata_location());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("BuildInCatalog - Fragment retrieved successfully");
                assertNotNull(response.getBody());
                
                String fragment = response.getBody();
                assertFalse(fragment.isEmpty());
                
                String etag = response.getHeaders().getFirst("ETag");
                assertNotNull(etag);
                System.out.println("BuildInCatalog - Fragment ETag: " + etag);
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("BuildInCatalog - Table not found for getFragment");
            } else {
                System.out.println("BuildInCatalog - Error getting fragment: " + e.getStatusCode());
                System.out.println("Response: " + e.getResponseBodyAsString());
            }
        }
    }

    @Test
    public void test6_CreateTableWithCustomLocation() throws Exception {
        String customTableName = "buildin_custom_location_" + System.currentTimeMillis();
        String url = String.format("%s/%s/tables/create", baseUrl, prefix);

        Map<String, Object> request = createBuildInCatalogRequestConfig("false", format_metadata_location());
        request.put("name", customTableName);
        
        // Add custom location for BuildInCatalog
        request.put("location", "/warehouse/builtin/" + namespace + "/" + customTableName);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("BuildInCatalog - Table with custom location created successfully");
                
                Map<String, Object> responseBody = objectMapper.readValue(response.getBody(), Map.class);
                Map<String, Object> metadata = (Map<String, Object>) responseBody.get("metadata");
                
                String tableLocation = (String) metadata.get("location");
                assertTrue(tableLocation.contains("builtin"));
                System.out.println("BuildInCatalog - Custom table location: " + tableLocation);
            }
            
        } catch (HttpClientErrorException e) {
            System.out.println("BuildInCatalog - Error creating table with custom location: " + e.getStatusCode());
            System.out.println("Response: " + e.getResponseBodyAsString());
        }
    }

    /**
     * The builtin catalog does support dropTable -- it delegates rather than
     * throwing, unlike renameTable -- so this drops the table created earlier
     * in the sequence and expects it to work.
     *
     * <p>This replaces a test7 that claimed to prove both drop and rename were
     * "unsupported operations". It proved neither: its URL carried a
     * {@code /namespace/{ns}/} segment the controller never had, so it only
     * ever measured the 404 of a missing route. Rename's absence from the REST
     * surface is now asserted in IcebergRestControllerWebTest, where it costs
     * no infrastructure.
     */
    @Test
    public void test7_DropTable() throws Exception {
        String dropUrl = String.format("%s/%s/tables/%s/drop", baseUrl, prefix, newTableName);

        Map<String, Object> request = createBuildInCatalogRequestConfig("true", format_metadata_location());
        request.put("namespace", namespace);
        request.put("purgeRequested", true);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(dropUrl, entity, String.class);
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
    }

    @Test
    public void test8_CreateTableWithoutWarehouseLocation() throws Exception {
        String noWarehouseTableName = "buildin_no_warehouse_" + System.currentTimeMillis();
        String url = String.format("%s/%s/tables/create", baseUrl, prefix);

        // Create request without warehouse location to test BuildInCatalog fallback
        Map<String, Object> request = createBuildInCatalogRequestConfigWithoutWarehouse();
        request.put("name", noWarehouseTableName);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("BuildInCatalog - Table created without warehouse location");
                
                Map<String, Object> responseBody = objectMapper.readValue(response.getBody(), Map.class);
                Map<String, Object> metadata = (Map<String, Object>) responseBody.get("metadata");
                
                // Should use BuildInCatalog's default behavior
                assertNotNull(metadata.get("location"));
                System.out.println("BuildInCatalog - Default location used: " + metadata.get("location"));
            }
            
        } catch (HttpClientErrorException e) {
            System.out.println("BuildInCatalog - Error creating table without warehouse: " + e.getStatusCode());
            System.out.println("Response: " + e.getResponseBodyAsString());
        }
    }

    /**
     * Create test request configuration for BuildInCatalog
     */
    private Map<String, Object> createBuildInCatalogRequestConfig(String table_exists, String metadata_location_str) {
        Map<String, Object> request = new HashMap<>();

        // Every table endpoint reads the namespace out of the body, not the URL.
        request.put("namespace", namespace);

        // IcebergConfig for BuildInCatalog
        Map<String, Object> icebergConfig = new HashMap<>();

        // IcebergCatalogConfig - configured for BuildInCatalog
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put("server_type", "builtin");  // Use builtin catalog type
        catalogConfig.put("hive_metastore_uri", "thrift://hive-metastore:9083");
        catalogConfig.put("auth_method", "simple");
        catalogConfig.put("warehouse_location_prefix", "s3a://warehouse/hive_location");
        icebergConfig.put("IcebergCatalogConfig", catalogConfig);

        // IcebergVolumeConfig - S3 compatible storage
        Map<String, Object> volumeConfig = new HashMap<>();
        volumeConfig.put("volume_server_type", "s3");
        volumeConfig.put("volume_endpoint", "http://minio:9000");
        volumeConfig.put("volume_region", "us-east-1");
        volumeConfig.put("bucket_name", "builtin-warehouse");
        volumeConfig.put("path_style_access", true);
        volumeConfig.put("access_key_id", "admin");
        volumeConfig.put("secret_access_key", "password");
        icebergConfig.put("IcebergVolumeConfig", volumeConfig);

        // IcebergAdditionalConfig with FileIOConfig
        Map<String, Object> additionalConfig = new HashMap<>();
        additionalConfig.put("totalSegment", "1");
        additionalConfig.put("splitSize", "128");
        additionalConfig.put("filterString", "");
        
        // FileIOConfig optimized for BuildInCatalog
        Map<String, Object> fileIOConfig = new HashMap<>();
        Map<String, Object> gopherConfig = new HashMap<>();
        Map<String, Object> common = new HashMap<>();
        common.put("worker_path", "/tmp/gopher/builtin/worker");
        common.put("connect_path", "/tmp/gopher/builtin/connect");
        common.put("ufs_type", "S3A");
        common.put("cache_strategy", "GOPHER_CACHE");
        common.put("gopher_mode", "GOPHER_NORMAL");
        common.put("log_level", "GOPHER_INFO");
        gopherConfig.put("common", common);
        fileIOConfig.put("gopherConfig", gopherConfig);
        
        Map<String, Object> properties = new HashMap<>();
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.access-key-id", "admin");
        properties.put("s3.secret-access-key", "password");
        properties.put("s3.path-style-access", "true");
        fileIOConfig.put("properties", properties);
        
        additionalConfig.put("fileIOConfig", fileIOConfig);
        icebergConfig.put("IcebergAdditionalConfig", additionalConfig);

        request.put("IcebergConfig", icebergConfig);

        // Schema definition
        request.put("schema", createTestSchema());

        Map<String, Object> buildInProp = new HashMap<>();
        buildInProp.put("buildInCatalog.table_exists", table_exists);
        buildInProp.put("buildInCatalog.metadata_location", metadata_location_str);

        request.put("properties", buildInProp);
        return request;
    }

    /**
     * Create BuildInCatalog config without warehouse location
     */
    private Map<String, Object> createBuildInCatalogRequestConfigWithoutWarehouse() {
        Map<String, Object> request = createBuildInCatalogRequestConfig("true", format_metadata_location());
        
        // Remove warehouse location from catalog config
        Map<String, Object> icebergConfig = (Map<String, Object>) request.get("IcebergConfig");
        Map<String, Object> catalogConfig = (Map<String, Object>) icebergConfig.get("IcebergCatalogConfig");
        catalogConfig.remove("catalog_location");
        
        return request;
    }

    /**
     * Create test schema for BuildInCatalog tables
     */
    private Map<String, Object> createTestSchema() {
        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "struct");
        schema.put("schema-id", 0);

        List<Map<String, Object>> fields = new ArrayList<>();
        
        // ID field
        Map<String, Object> idField = new HashMap<>();
        idField.put("id", 1);
        idField.put("name", "id");
        idField.put("type", "long");
        idField.put("required", true);
        fields.add(idField);

        // Name field
        Map<String, Object> nameField = new HashMap<>();
        nameField.put("id", 2);
        nameField.put("name", "name");
        nameField.put("type", "string");
        nameField.put("required", true);
        fields.add(nameField);

        // Age field
        Map<String, Object> ageField = new HashMap<>();
        ageField.put("id", 3);
        ageField.put("name", "age");
        ageField.put("type", "int");
        ageField.put("required", false);
        fields.add(ageField);

        // BuildInCatalog specific field
        Map<String, Object> catalogField = new HashMap<>();
        catalogField.put("id", 4);
        catalogField.put("name", "catalog_type");
        catalogField.put("type", "string");
        catalogField.put("required", false);
        fields.add(catalogField);

        schema.put("fields", fields);
        return schema;
    }
}
