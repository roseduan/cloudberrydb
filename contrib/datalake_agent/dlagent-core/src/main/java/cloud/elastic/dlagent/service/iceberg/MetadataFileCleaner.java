/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.service.iceberg;

import cloud.elastic.dlagent.api.model.CleanupFromMetadataResponse;
import cloud.elastic.dlagent.service.ServiceResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cloud.elastic.dlagent.service.iceberg.IcebergConfigurationBuilder.putIfPresent;

/**
 * Deletes every object an Iceberg metadata tree references, for the
 * deletion-queue consumer in datalake_fdw
 * ({@code agent_cli_wrapper_cleanup_metadata}).
 *
 * <p>Lives in core because the gRPC transport has to offer the same operation,
 * and walking a snapshot tree needs nothing from HTTP. The method bodies are
 * those the REST controller always ran, with one deliberate exception
 * recorded below.
 *
 * <p>THE ONE SIGNATURE CHANGE. {@code cleanupMetadataTree} returned a
 * {@code ResponseEntity}; it now returns a {@link ServiceResult}, because
 * ResponseEntity is a Spring type and core may not import Spring. The two
 * substitutions are byte-preserving rather than merely similar:
 * <ul>
 * <li>{@code createErrorResponse(msg, type, code)} became
 *     {@link ServiceResult#failure(String, String, int)}, whose body is
 *     character-for-character the one that method built -- same HashMap, same
 *     three keys, same insertion order -- so the serialized error is identical.
 * <li>{@code ResponseEntity.ok(resp)} became {@link ServiceResult#success},
 *     carrying the same {@code resp} object, so the success body is the same
 *     object serialized by the same mapper.
 * </ul>
 */
@Named
@Singleton
@Slf4j
public class MetadataFileCleaner {

    /*
     * Two references to the same logger, deliberately. The bodies below are
     * verbatim the ones IcebergRestController ran, which carried BOTH a lombok
     * @Slf4j-generated `log` and an explicitly declared `LOG`, and different
     * methods used different ones. Providing both keeps every body identical
     * to that code, so it is evident that their behaviour is the REST
     * controller's. Collapsing them to one name would be a cosmetic edit
     * inside bodies that are meant to stay identical.
     *
     * Note the one observable change inherent to any such move: the logger
     * CATEGORY becomes this class instead of IcebergRestController, so these
     * lines are attributed differently in the log file. Response bytes are
     * unaffected.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MetadataFileCleaner.class);

    /**
     * Recognized {@code fileIOConfig.type} values posted by the datalake_fdw
     * consumer.  Kept as named constants so the supported storage schemes are
     * managed in one place rather than scattered as string literals.
     */
    private static final String FILEIO_CONFIG_TYPE_KEY = "type";
    private static final String FILEIO_TYPE_S3 = "s3";
    private static final String FILEIO_TYPE_HDFS = "hdfs";
    private static final String FILEIO_TYPE_ABFSS = "abfss";

    /**
     * Default HDFS_PORT when the volume options give a bare host.
     */
    private static final String HDFS_DEFAULT_PORT = "8020";

    /**
     * Builds the {@code fs.defaultFS} value for an HDFS cleanup request.
     *
     * <p>HA deployments name a nameservice instead of a host, and the
     * {@code dfs.*} properties set alongside resolve it; a plain deployment
     * needs host and port, which reach us either spliced into
     * {@code namenodes} or as the separate {@code port} option.  Returns null
     * when neither is present, so the caller leaves fs.defaultFS alone rather
     * than setting a broken value.
     */
    static String hdfsDefaultFs(Map<String, String> cfg) {
        String nameservices = cfg.get("nameservices");
        if (nameservices != null && !nameservices.isEmpty()) {
            return "hdfs://" + nameservices;
        }

        String namenodes = cfg.get("namenodes");
        if (namenodes == null || namenodes.isEmpty()) {
            return null;
        }
        if (namenodes.indexOf(',') >= 0) {
            /*
             * Several namenodes and no nameservice to resolve them: there is
             * no single authority to put in a URI.  Leave fs.defaultFS unset
             * rather than build "hdfs://nn1:8020,nn2:8020", which Hadoop
             * cannot parse -- a broken value fails louder but no later than
             * the absent one, and it fails for a reason nobody can read.
             */
            return null;
        }
        if (namenodes.lastIndexOf(':') >= 0) {
            return "hdfs://" + namenodes;
        }

        String port = cfg.get("port");
        if (port == null || port.isEmpty()) {
            port = HDFS_DEFAULT_PORT;
        }
        return "hdfs://" + namenodes + ":" + port;
    }

    public ServiceResult<Object> cleanupMetadataTree(FileIO fileIO,
                                                  String metadataPath) {
        // 2. Parse the metadata.json so we can walk the snapshot tree.
        List<Map<String, String>> failed = new ArrayList<>();
        int deletedCount = 0;

        // 2. Parse the metadata.json and collect the EXACT set of files it
        //    references -- every retained snapshot's manifest list ->
        //    manifests -> data/delete files, plus the metadata.json chain and
        //    statistics files.  We delete only these authoritative paths and
        //    never a blind prefix listing, so a shared or mis-derived prefix
        //    can never take out files belonging to another table.
        String tableRoot = deriveTableRoot(metadataPath);

        java.util.Set<String> allowedRoots = new java.util.LinkedHashSet<>();
        addManagedRoot(allowedRoots, tableRoot);

        TableMetadata metadata;
        try {
            InputFile input = fileIO.newInputFile(metadataPath);
            metadata = TableMetadataParser.read(fileIO, input);
        } catch (Exception e) {
            return ServiceResult.failure(
                "failed to parse metadata.json at " + metadataPath + ": "
                    + e.getMessage(),
                "BadRequestException", 400);
        }
        addManagedRoot(allowedRoots, metadata.location());

        Map<Integer, PartitionSpec> specsById = metadata.specsById();

        // Insertion order: manifests/data files are queued before the
        // metadata.json that references them; the root metadata.json is added
        // last so a mid-flight crash leaves a resumable tree.
        java.util.Set<String> toDelete = new java.util.LinkedHashSet<>();

        // 2a. Walk every retained snapshot: manifest list -> manifests ->
        //     data files / delete files.
        for (Snapshot snapshot : metadata.snapshots()) {
            String manifestList = snapshot.manifestListLocation();
            if (manifestList != null) {
                toDelete.add(manifestList);
            }

            List<ManifestFile> manifests;
            try {
                manifests = snapshot.allManifests(fileIO);
            } catch (Exception e) {
                recordFailure(failed, "manifest-list:" + snapshot.snapshotId(), e);
                continue;
            }

            for (ManifestFile manifest : manifests) {
                toDelete.add(manifest.path());
                try (CloseableIterable<? extends ContentFile<?>> entries =
                        manifest.content() == ManifestContent.DELETES
                            ? ManifestFiles.readDeleteManifest(manifest, fileIO,
                                                               specsById)
                            : ManifestFiles.read(manifest, fileIO, specsById)) {
                    for (ContentFile<?> cf : entries) {
                        toDelete.add(cf.path().toString());
                    }
                } catch (Exception e) {
                    recordFailure(failed, manifest.path(), e);
                }
            }
        }

        // 2b. The metadata.json chain (previous metadata files) and any
        //     statistics files referenced by the current metadata.
        for (TableMetadata.MetadataLogEntry prev : metadata.previousFiles()) {
            toDelete.add(prev.file());
        }
        for (StatisticsFile stat : metadata.statisticsFiles()) {
            toDelete.add(stat.path());
        }

        // The root metadata.json itself is deleted last.
        toDelete.add(metadataPath);

        // 3. Delete each referenced file.  An already-gone file counts as
        //    success (idempotent retry).  Anything resolving outside the
        //    table's managed root is skipped for safety.  Real delete errors
        //    land in failed[]; the consumer retries / dead-letters on a
        //    non-empty failed[].
        for (String pth : toDelete) {
            if (!underAnyRoot(pth, allowedRoots)) {
                Map<String, String> err = new HashMap<>();
                err.put("path", pth);
                err.put("error", "outside managed location; skipped for safety");
                failed.add(err);
                continue;
            }
            try {
                fileIO.deleteFile(pth);
                deletedCount++;
            } catch (NotFoundException nfe) {
                deletedCount++;
            } catch (Exception e) {
                recordFailure(failed, pth, e);
            }
        }

        CleanupFromMetadataResponse resp = new CleanupFromMetadataResponse();
        resp.setDeletedCount(deletedCount);
        resp.setFailed(failed);

        log.info("Cleanup of {} finished: parsed snapshot tree, deleted={}, "
                 + "failed={}", metadataPath, deletedCount, failed.size());

        return ServiceResult.success(resp);
    }
    private static String deriveTableRoot(String metadataPath) {
        if (metadataPath == null || metadataPath.isEmpty()) {
            return null;
        }
        int i = metadataPath.lastIndexOf("/metadata/");
        if (i > 0) {
            return metadataPath.substring(0, i);
        }
        int s = metadataPath.lastIndexOf('/');
        return s > 0 ? metadataPath.substring(0, s) : null;
    }
    private static void addManagedRoot(java.util.Set<String> allowedRoots,
                                       String root) {
        if (root == null || root.isEmpty()) {
            return;
        }
        String r = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        allowedRoots.add(r);
    }
    public static void closeFileIOQuietly(FileIO fileIO) {
        try {
            fileIO.close();
        } catch (Exception e) {
            log.warn("Failed to close FileIO after cleanup; ignoring", e);
        }
    }
    /**
     * Public because the {@code /files/delete} endpoint records its own
     * per-file failures in the same shape.
     */
    public static void recordFailure(List<Map<String, String>> failed,
                                      String path, Exception e) {
        Map<String, String> err = new HashMap<>();
        err.put("path", path);
        err.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        failed.add(err);
    }
    private static String canonRoot(String p) {
        if (p == null) {
            return "";
        }
        String q = p;
        if (q.startsWith("s3a://")) {
            q = "s3://" + q.substring("s3a://".length());
        }
        if (q.endsWith("/")) {
            q = q.substring(0, q.length() - 1);
        }
        return q;
    }
    private static boolean underAnyRoot(String path, java.util.Set<String> roots) {
        String cp = canonRoot(path);
        for (String r : roots) {
            String cr = canonRoot(r);
            if (cp.equals(cr) || cp.startsWith(cr + "/")) {
                return true;
            }
        }
        return false;
    }
    public FileIO buildFileIOForCleanup(Map<String, String> cfg) {
        Configuration conf = new Configuration();
        String type = cfg.getOrDefault(FILEIO_CONFIG_TYPE_KEY, FILEIO_TYPE_S3);

        if (FILEIO_TYPE_S3.equals(type)) {
            // Hadoop S3A keys.  endpoint / region / path_style_access are
            // optional; access_key_id / secret_access_key are required for
            // private buckets.
            putIfPresent(conf, cfg, "endpoint",          "fs.s3a.endpoint");
            putIfPresent(conf, cfg, "region",            "fs.s3a.endpoint.region");
            putIfPresent(conf, cfg, "path_style_access", "fs.s3a.path.style.access");
            putIfPresent(conf, cfg, "access_key_id",     "fs.s3a.access.key");
            putIfPresent(conf, cfg, "secret_access_key", "fs.s3a.secret.key");
            // Iceberg metadata is written with the "s3a://" scheme but the
            // data files referenced inside the manifests use the bare "s3://"
            // scheme.  Register S3AFileSystem for "s3" as well and mirror the
            // credentials, otherwise fileIO.deleteFile() on a "s3://..." data
            // file fails with "Failed to get file system" and the data files
            // are left orphaned (only metadata gets cleaned up).
            conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            putIfPresent(conf, cfg, "endpoint",          "fs.s3.endpoint");
            putIfPresent(conf, cfg, "region",            "fs.s3.endpoint.region");
            putIfPresent(conf, cfg, "path_style_access", "fs.s3.path.style.access");
            putIfPresent(conf, cfg, "access_key_id",     "fs.s3.access.key");
            putIfPresent(conf, cfg, "secret_access_key", "fs.s3.secret.key");
        } else if (FILEIO_TYPE_HDFS.equals(type)) {
            putIfPresent(conf, cfg, "namenodes",          "dfs.namenode.rpc-address");
            putIfPresent(conf, cfg, "auth_method",        "hadoop.security.authentication");
            putIfPresent(conf, cfg, "rpc_protection",     "hadoop.rpc.protection");
            putIfPresent(conf, cfg, "nameservices",       "dfs.nameservices");
            putIfPresent(conf, cfg, "ha_namenodes",       "dfs.ha.namenodes");
            putIfPresent(conf, cfg, "namenode_rpc_address",
                         "dfs.namenode.rpc-address");
            putIfPresent(conf, cfg, "failover_proxy_provider",
                         "dfs.client.failover.proxy.provider");
            /*
             * A volume's paths are authority-less ("hdfs:///warehouse/..."):
             * the namenode lives in the volume options, not in the URI.  With
             * no fs.defaultFS, Hadoop cannot tell which cluster such a path
             * belongs to and deleteFile() fails with "Failed to get file
             * system", leaving the files orphaned -- the same class of bug the
             * s3/s3a scheme handling above exists to prevent.
             */
            String defaultFs = hdfsDefaultFs(cfg);
            if (defaultFs != null) {
                conf.set("fs.defaultFS", defaultFs);
            }
        } else if (FILEIO_TYPE_ABFSS.equals(type)) {
            // Minimal abfss support; expand if/when production paths hit it.
            putIfPresent(conf, cfg, "tenant_id", "fs.azure.account.oauth2.client.endpoint");
        } else {
            throw new IllegalArgumentException(
                "Unsupported fileIOConfig.type: " + type);
        }

        return new HadoopFileIO(conf);
    }
}
