-- Hudi metadata table regression data for issue #873.
-- Keep the metadata table enabled and force frequent metadata compaction so
-- the MDT files partition contains a base HFile, which triggers the Hadoop
-- getReadStatistics ABI mismatch when metadata_table_enable=true.

DROP TABLE IF EXISTS hudi_mdt_repro;
CREATE TABLE hudi_mdt_repro
USING hudi
OPTIONS (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'ts',
    hoodie.table.name = 'hudi_mdt_repro',
    hoodie.datasource.write.table.name = 'hudi_mdt_repro',
    hoodie.datasource.write.recordkey.field = 'id',
    hoodie.datasource.write.precombine.field = 'ts',
    hoodie.datasource.write.keygenerator.class = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
    hoodie.metadata.enable = 'true',
    hoodie.metadata.compact.max.delta.commits = '2'
)
AS SELECT * FROM (
    SELECT CAST(1 AS BIGINT) as id, 'alice' as name, CAST(1 AS BIGINT) as ts
    UNION ALL SELECT 2, 'bob', 2
    UNION ALL SELECT 3, 'carol', 3
) t;

-- The CTAS commit plus this insert commit are enough to compact the MDT
-- files partition into a base HFile with compact.max.delta.commits=2.
INSERT INTO hudi_mdt_repro
SELECT * FROM (
    SELECT CAST(2 AS BIGINT) as id, 'bob_v2' as name, CAST(20 AS BIGINT) as ts
    UNION ALL SELECT 4, 'dave', 4
) t;
