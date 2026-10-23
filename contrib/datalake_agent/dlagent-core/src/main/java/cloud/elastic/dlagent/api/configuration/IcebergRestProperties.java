/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.api.configuration;

import lombok.Data;

/**
 * Iceberg operation defaults, bound from the {@code iceberg.rest} prefix.
 *
 * <p>Plain POJO: the binding lives in each composition root, because
 * {@code @ConfigurationProperties} is a spring-boot type that dlagent-core may
 * not reference. dlagent-rest declares
 * {@code @Bean @ConfigurationProperties(prefix = "iceberg.rest")}, which binds
 * this exact class with the same prefix and therefore the same values as
 * before; dlagent-grpc binds an instance filled in by its own configuration
 * parser. {@link GopherConfigurationProperties} follows the same pattern.
 *
 * <p>It lives in core because {@code planFileGroups}, a core operation, reads
 * two of these values. THE PREFIX IS DELIBERATELY STILL
 * {@code iceberg.rest} even though nothing about these values is REST-specific:
 * it is what existing {@code application.properties} files use, and both jars
 * are required to read the same configuration format unchanged. Renaming it
 * would be a silent configuration break for anyone who set it.
 */
@Data
public class IcebergRestProperties {

    /** The property prefix these values are bound from. */
    public static final String PROPERTY_PREFIX = "iceberg.rest";

    /**
     * Whether Iceberg REST API is enabled
     */
    private boolean enabled = true;

    /**
     * Base path for Iceberg REST API
     */
    private String basePath = "/api";

    /**
     * Default target file size in MB for vacuum compaction.
     * Used as fallback when the caller does not supply this parameter.
     * Should be kept in sync with GUC datalake.iceberg_vacuum_rewrite_target_file_size_mb.
     */
    private int vacuumTargetFileSizeMb = 512;

    /**
     * Default minimum number of input files for vacuum compaction.
     * Used as fallback when the caller does not supply this parameter.
     * Should be kept in sync with GUC datalake.iceberg_vacuum_compact_min_input_files.
     */
    private int vacuumMinInputFiles = 5;
}
