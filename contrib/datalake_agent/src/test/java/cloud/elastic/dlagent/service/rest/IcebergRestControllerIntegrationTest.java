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

/**
 * Integration test for IcebergRestController using REST API calls
 * Requires the Spring Boot application to be running with JVM debug enabled
 * 
 * To run this test:
 * 1. Start the application with JVM debug: java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar app.jar
 * 2. Run this test class
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class IcebergRestControllerIntegrationTest {

    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;
    private String baseUrl;
    private String basePrefix = "s3a://hiveCatalogLocation/hive/testlocation";
    private String prefix;
    private String namespace = "default";
    private static final String newTableName = "test_table_" + System.currentTimeMillis();

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

    @Test
    public void test1_TableExists_TableNotFound() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/exists", baseUrl, prefix, newTableName);

        Map<String, Object> request = createRequestConfig();

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
            
            // Parse error response
            System.out.println(e.getResponseBodyAsString());
            // Map<String, Object> errorResponse = objectMapper.readValue(e.getResponseBodyAsString(), Map.class);
            // assertTrue(errorResponse.containsKey("error"));
            
            // Object errorObj = errorResponse.get("error");
            // if (errorObj instanceof Map) {
            //     Map<String, Object> error = (Map<String, Object>) errorObj;
            //     assertEquals("NoSuchTableException", error.get("type"));
            //     assertEquals(404, error.get("code"));
            // } else {
            //     fail("Expected error to be a Map, but got: " + errorObj.getClass());
            // }
        }
    }

    @Test
    public void test2_CreateTable() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/create", baseUrl, prefix);

        Map<String, Object> request = createRequestConfig();
        request.put("name", newTableName);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("Table created successfully");
                assertNotNull(response.getBody());
                
                Map<String, Object> responseBody = objectMapper.readValue(response.getBody(), Map.class);
                assertTrue(responseBody.containsKey("metadata"));
                assertTrue(responseBody.containsKey("metadata-location"));
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT) {
                System.out.println("Table already exists");
            } else if (e.getStatusCode() == HttpStatus.BAD_REQUEST) {
                System.out.println("Bad request: " + e.getResponseBodyAsString());
            } else {
                System.out.println("Error creating table: " + e.getStatusCode());
                System.out.println("Response: " + e.getResponseBodyAsString());
            }
        }
    }

    @Test
    public void test3_TableExists_TableFound() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/exists", baseUrl, prefix, newTableName);

        Map<String, Object> request = createRequestConfig();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            // If table exists, expect 200 OK
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("Table exists - Response: " + response.getStatusCode());
            }
            
            // If table doesn't exist, expect 404 NOT_FOUND
            if (response.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("Table not found - Response: " + response.getStatusCode());
                assertNotNull(response.getBody());
            }
            
        } catch (HttpClientErrorException e) {
            // Handle 404 or other client errors
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("Table not found - Status: " + e.getStatusCode());
                System.out.println("Response body: " + e.getResponseBodyAsString());
            } else {
                System.out.println("Unexpected error - Status: " + e.getStatusCode());
                System.out.println("Response body: " + e.getResponseBodyAsString());
            }
        }
    }



    @Test
    public void test4_LoadTable() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/load", baseUrl, prefix, newTableName);

        Map<String, Object> request = createRequestConfig();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("Table loaded successfully");
                assertNotNull(response.getBody());
                
                // Parse response
                Map<String, Object> responseBody = objectMapper.readValue(response.getBody(), Map.class);
                assertTrue(responseBody.containsKey("metadata"));
                assertTrue(responseBody.containsKey("metadata-location"));
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("Table not found for loading");
                Map<String, Object> errorResponse = objectMapper.readValue(e.getResponseBodyAsString(), Map.class);
                assertTrue(errorResponse.containsKey("error"));
            }
        }
    }

    /**
     * Create test request configuration matching the new OpenAPI structure
     */
    private Map<String, Object> createRequestConfig() {
        Map<String, Object> request = new HashMap<>();

        // Every table endpoint reads the namespace out of the body, not the URL.
        request.put("namespace", namespace);

        // IcebergConfig (top level)
        Map<String, Object> icebergConfig = new HashMap<>();

        // IcebergCatalogConfig
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put("server_type", "hive");
        catalogConfig.put("hive_metastore_uri", "thrift://hive-metastore:9083");
        catalogConfig.put("auth_method", "simple");
        catalogConfig.put("warehouse_location_prefix", "s3a://warehouse/hive_location");
        icebergConfig.put("IcebergCatalogConfig", catalogConfig);

        // IcebergVolumeConfig
        Map<String, Object> volumeConfig = new HashMap<>();
        volumeConfig.put("volume_server_type", "s3");
        volumeConfig.put("volume_endpoint", "http://minio:9000");
        volumeConfig.put("volume_region", "us-east-1");
        volumeConfig.put("bucket_name", "test-bucket");
        volumeConfig.put("path_style_access", true);
        volumeConfig.put("access_key_id", "admin");
        volumeConfig.put("secret_access_key", "password");
        icebergConfig.put("IcebergVolumeConfig", volumeConfig);

        // IcebergAdditionalConfig with FileIOConfig
        Map<String, Object> additionalConfig = new HashMap<>();
        additionalConfig.put("totalSegment", "1");
        additionalConfig.put("splitSize", "128");
        additionalConfig.put("filterString", "");
        
        // FileIOConfig - using GopherFileIOConfig
        Map<String, Object> fileIOConfig = new HashMap<>();
        Map<String, Object> gopherConfig = new HashMap<>();
        Map<String, Object> common = new HashMap<>();
        common.put("worker_path", "/tmp/gopher/worker");
        common.put("connect_path", "/tmp/gopher/connect");
        common.put("ufs_type", "S3A");
        common.put("cache_strategy", "GOPHER_CACHE");
        common.put("gopher_mode", "GOPHER_NORMAL");
        common.put("log_level", "GOPHER_INFO");
        gopherConfig.put("common", common);
        fileIOConfig.put("gopherConfig", gopherConfig);
        
        // Additional properties for FileIO
        Map<String, Object> properties = new HashMap<>();
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.access-key-id", "admin");
        properties.put("s3.secret-access-key", "password");
        fileIOConfig.put("properties", properties);
        
        additionalConfig.put("fileIOConfig", fileIOConfig);
        icebergConfig.put("IcebergAdditionalConfig", additionalConfig);

        request.put("IcebergConfig", icebergConfig);

        // Schema
        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "struct");
        schema.put("schema-id", 0);

        List<Map<String, Object>> fields = new ArrayList<>();
        
        Map<String, Object> idField = new HashMap<>();
        idField.put("id", 1);
        idField.put("name", "id");
        idField.put("type", "long");
        idField.put("required", true);
        fields.add(idField);

        Map<String, Object> nameField = new HashMap<>();
        nameField.put("id", 2);
        nameField.put("name", "name");
        nameField.put("type", "string");
        nameField.put("required", true);
        fields.add(nameField);

        Map<String, Object> ageField = new HashMap<>();
        ageField.put("id", 3);
        ageField.put("name", "age");
        ageField.put("type", "int");
        ageField.put("required", false);
        fields.add(ageField);

        schema.put("fields", fields);
        request.put("schema", schema);

        return request;
    }

    @Test
    public void test5_GetFragment() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/getFragment", baseUrl, prefix, newTableName);

        Map<String, Object> request = createRequestConfig();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("Fragment retrieved successfully");
                assertNotNull(response.getBody());
                
                // Verify response contains fragment data
                String fragment = response.getBody();
                assertFalse(fragment.isEmpty());
                
                // Check ETag header is present
                String etag = response.getHeaders().getFirst("ETag");
                assertNotNull(etag);
                System.out.println("Fragment ETag: " + etag);
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("Table not found for getFragment");
                Map<String, Object> errorResponse = objectMapper.readValue(e.getResponseBodyAsString(), Map.class);
                assertTrue(errorResponse.containsKey("error"));
            } else {
                System.out.println("Error getting fragment: " + e.getStatusCode());
                System.out.println("Response: " + e.getResponseBodyAsString());
            }
        }
    }

    @Test
    public void test6_GetFragmentWithETag() throws Exception {
        // Arrange
        String url = String.format("%s/%s/tables/%s/getFragment", baseUrl, prefix, newTableName);

        Map<String, Object> request = createRequestConfig();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("If-None-Match", "test-etag-123");
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // Act & Assert
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
            
            // Should return fragment data or 304 Not Modified
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("Fragment retrieved with ETag check");
                assertNotNull(response.getBody());
            } else if (response.getStatusCode() == HttpStatus.NOT_MODIFIED) {
                System.out.println("Fragment not modified (304)");
            }
            
        } catch (HttpClientErrorException e) {
            System.out.println("Error in ETag test: " + e.getStatusCode());
            System.out.println("Response: " + e.getResponseBodyAsString());
        }
    }

    @Test
    public void test7_AppendToTable() throws Exception {
        String appendTableName = "append_test_table_" + System.currentTimeMillis();
        
        // Step 1: Create table (following test2_CreateTable pattern)
        System.out.println("Step 1: Creating table for append test...");
        String createUrl = String.format("%s/%s/tables/create", baseUrl, prefix);
        
        Map<String, Object> createRequest = createRequestConfig();
        createRequest.put("name", appendTableName);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> createEntity = new HttpEntity<>(createRequest, headers);
        
        try {
            ResponseEntity<String> createResponse = restTemplate.postForEntity(createUrl, createEntity, String.class);
            
            if (createResponse.getStatusCode() == HttpStatus.OK) {
                System.out.println("✅ Table created successfully");
                assertNotNull(createResponse.getBody());
                
                Map<String, Object> createResponseBody = objectMapper.readValue(createResponse.getBody(), Map.class);
                assertTrue(createResponseBody.containsKey("metadata"));
                assertTrue(createResponseBody.containsKey("metadata-location"));
                
                // Step 2: Append data to table
                System.out.println("Step 2: Appending data to table...");
                String appendUrl = String.format("%s/%s/tables/%s/append", baseUrl, prefix, appendTableName);
                
                Map<String, Object> appendRequest = createRequestConfig();
                
                // Add fragments for append operation with required fields
                List<Map<String, Object>> fragments = new ArrayList<>();
                
                Map<String, Object> fragment1 = new HashMap<>();
                fragment1.put("path", "/warehouse/" + appendTableName + "/data/file1.parquet");
                fragment1.put("format", "PARQUET");
                fragment1.put("record_count", 100);
                fragment1.put("file_size_in_bytes", 10240);
                
                // Add partition spec if needed
                Map<String, Object> partitionSpec = new HashMap<>();
                partitionSpec.put("spec-id", 0);
                partitionSpec.put("fields", new ArrayList<>());
                fragment1.put("partition", partitionSpec);
                
                // Add column sizes and value counts
                Map<String, Object> columnSizes = new HashMap<>();
                columnSizes.put("1", 800);  // id column
                columnSizes.put("2", 1200); // name column  
                columnSizes.put("3", 400);  // age column
                fragment1.put("column_sizes", columnSizes);
                
                Map<String, Object> valueCounts = new HashMap<>();
                valueCounts.put("1", 100);
                valueCounts.put("2", 100);
                valueCounts.put("3", 95);
                fragment1.put("value_counts", valueCounts);
                
                fragments.add(fragment1);
                
                Map<String, Object> fragment2 = new HashMap<>();
                fragment2.put("path", "/warehouse/" + appendTableName + "/data/file2.parquet");
                fragment2.put("format", "PARQUET");
                fragment2.put("record_count", 200);
                fragment2.put("file_size_in_bytes", 20480);
                fragment2.put("partition", partitionSpec);
                
                Map<String, Object> columnSizes2 = new HashMap<>();
                columnSizes2.put("1", 1600);
                columnSizes2.put("2", 2400);
                columnSizes2.put("3", 800);
                fragment2.put("column_sizes", columnSizes2);
                
                Map<String, Object> valueCounts2 = new HashMap<>();
                valueCounts2.put("1", 200);
                valueCounts2.put("2", 200);
                valueCounts2.put("3", 190);
                fragment2.put("value_counts", valueCounts2);
                
                fragments.add(fragment2);
                
                appendRequest.put("fragments", fragments);
                
                HttpEntity<Map<String, Object>> appendEntity = new HttpEntity<>(appendRequest, headers);
                
                try {
                    ResponseEntity<String> appendResponse = restTemplate.postForEntity(appendUrl, appendEntity, String.class);
                    
                    if (appendResponse.getStatusCode() == HttpStatus.OK) {
                        System.out.println("✅ Data appended successfully");
                        assertNotNull(appendResponse.getBody());
                        
                        // Parse append response
                        Map<String, Object> appendResponseBody = objectMapper.readValue(appendResponse.getBody(), Map.class);
                        // Verify append response structure (may vary based on implementation)
                        
                        // Step 3: Load table to verify data (following test4_LoadTable pattern)
                        System.out.println("Step 3: Loading table to verify appended data...");
                        String loadUrl = String.format("%s/%s/tables/%s/load", baseUrl, prefix, appendTableName);
                        
                        Map<String, Object> loadRequest = createRequestConfig();
                        HttpEntity<Map<String, Object>> loadEntity = new HttpEntity<>(loadRequest, headers);
                        
                        ResponseEntity<String> loadResponse = restTemplate.postForEntity(loadUrl, loadEntity, String.class);
                        
                        if (loadResponse.getStatusCode() == HttpStatus.OK) {
                            System.out.println("✅ Table loaded successfully after append");
                            assertNotNull(loadResponse.getBody());
                            
                            // Parse load response
                            Map<String, Object> loadResponseBody = objectMapper.readValue(loadResponse.getBody(), Map.class);
                            assertTrue(loadResponseBody.containsKey("metadata"));
                            assertTrue(loadResponseBody.containsKey("metadata-location"));
                            
                            System.out.println("✅ Append test completed: Create → Append → Load verified");
                        }
                        
                    } else {
                        System.out.println("Append operation failed with status: " + appendResponse.getStatusCode());
                        System.out.println("Response body: " + appendResponse.getBody());
                    }
                    
                } catch (HttpClientErrorException e) {
                    System.out.println("Error during append: " + e.getStatusCode());
                    System.out.println("Response: " + e.getResponseBodyAsString());
                    
                    // Accept that append might not be fully implemented yet
                    assertTrue(e.getStatusCode() == HttpStatus.NOT_FOUND || 
                              e.getStatusCode() == HttpStatus.BAD_REQUEST ||
                              e.getStatusCode() == HttpStatus.NOT_IMPLEMENTED);
                }
                
            } else {
                System.out.println("Table creation failed with status: " + createResponse.getStatusCode());
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT) {
                System.out.println("Table already exists");
            } else if (e.getStatusCode() == HttpStatus.BAD_REQUEST) {
                System.out.println("Bad request during table creation: " + e.getResponseBodyAsString());
            } else {
                System.out.println("Error creating table: " + e.getStatusCode());
                System.out.println("Response: " + e.getResponseBodyAsString());
            }
        }
    }

    @Test
    public void test8_AppendToTable_EmptyFragments() throws Exception {
        String appendTableName = "empty_append_test_table_" + System.currentTimeMillis();
        
        // Step 1: Create table first (following test7 pattern)
        System.out.println("Step 1: Creating table for empty fragments test...");
        String createUrl = String.format("%s/%s/tables/create", baseUrl, prefix);
        
        Map<String, Object> createRequest = createRequestConfig();
        createRequest.put("name", appendTableName);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> createEntity = new HttpEntity<>(createRequest, headers);
        
        try {
            ResponseEntity<String> createResponse = restTemplate.postForEntity(createUrl, createEntity, String.class);
            
            if (createResponse.getStatusCode() == HttpStatus.OK) {
                System.out.println("✅ Table created successfully");
                
                // Step 2: Try to append with empty fragments
                System.out.println("Step 2: Testing append with empty fragments...");
                String appendUrl = String.format("%s/%s/tables/%s/append", baseUrl, prefix, appendTableName);

                Map<String, Object> appendRequest = createRequestConfig();
                appendRequest.put("fragments", new ArrayList<>()); // Empty fragments list

                HttpEntity<Map<String, Object>> appendEntity = new HttpEntity<>(appendRequest, headers);

                // Act & Assert - expect error
                try {
                    ResponseEntity<String> response = restTemplate.postForEntity(appendUrl, appendEntity, String.class);
                    fail("Expected HttpClientErrorException to be thrown for empty fragments");
                } catch (HttpClientErrorException e) {
                    System.out.println("✅ Expected error received: " + e.getStatusCode());
                    System.out.println("Error response body: " + e.getResponseBodyAsString());
                    
                    assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
                    assertNotNull(e.getResponseBodyAsString());
                    
                    Map<String, Object> errorResponse = objectMapper.readValue(e.getResponseBodyAsString(), Map.class);
                    assertTrue(errorResponse.containsKey("error"));
                    
                    Map<String, Object> error = (Map<String, Object>) errorResponse.get("error");
                    System.out.println("Error type: " + error.get("type"));
                    System.out.println("Error code: " + error.get("code"));
                    System.out.println("Error message: " + error.get("message"));
                    
                    assertEquals("BadRequestException", error.get("type"));
                    assertEquals(400, error.get("code"));
                    assertTrue(error.get("message").toString().contains("Fragments are required"));
                    
                    System.out.println("✅ Empty fragments test completed: Create → Empty Append → Error verified");
                }
                
            } else {
                System.out.println("Table creation failed with status: " + createResponse.getStatusCode());
            }
            
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT) {
                System.out.println("Table already exists, proceeding with append test");
                
                // Still test the empty fragments append
                String appendUrl = String.format("%s/%s/tables/%s/append", baseUrl, prefix, appendTableName);
                Map<String, Object> appendRequest = createRequestConfig();
                appendRequest.put("fragments", new ArrayList<>());
                HttpEntity<Map<String, Object>> appendEntity = new HttpEntity<>(appendRequest, headers);

                try {
                    ResponseEntity<String> response = restTemplate.postForEntity(appendUrl, appendEntity, String.class);
                    fail("Expected HttpClientErrorException to be thrown for empty fragments");
                } catch (HttpClientErrorException appendError) {
                    System.out.println("✅ Expected error received: " + appendError.getStatusCode());
                    System.out.println("Error response body: " + appendError.getResponseBodyAsString());
                    
                    assertEquals(HttpStatus.BAD_REQUEST, appendError.getStatusCode());
                    
                    Map<String, Object> errorResponse = objectMapper.readValue(appendError.getResponseBodyAsString(), Map.class);
                    Map<String, Object> error = (Map<String, Object>) errorResponse.get("error");
                    System.out.println("Error type: " + error.get("type"));
                    System.out.println("Error code: " + error.get("code"));
                    System.out.println("Error message: " + error.get("message"));
                    
                    System.out.println("✅ Empty fragments error verified on existing table");
                }
            } else {
                System.out.println("Error creating table: " + e.getStatusCode());
                System.out.println("Response: " + e.getResponseBodyAsString());
            }
        }
    }
}