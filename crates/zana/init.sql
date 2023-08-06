CREATE TABLE nodes (
    id Int64 CODEC(T64, LZ4HC),
    cell3 UInt64,
    cell12 UInt64 CODEC(T64, Default),
    decimicro_lat Int32 CODEC(Delta, LZ4HC),
    decimicro_lon Int32 CODEC(Delta, LZ4HC),
    tags Map(UInt64, UInt64) CODEC(T64, LZ4HC)

) ENGINE = ReplacingMergeTree()
ORDER BY (cell12, id)
PARTITION BY cell3

CREATE TABLE paths (
    id Int64 CODEC(T64, LZ4HC),
    nodes Array(Int64) CODEC(Delta, LZ4HC),
    tags Map(UInt64, UInt64) CODEC(T64, LZ4HC)
    INDEX node_ids nodes TYPE Set(0)
) ENGINE = ReplacingMergeTree()
ORDER BY id

CREATE TABLE string_table (
    id UInt64,
    string String
) ENGINE = ReplacingMergeTree()
ORDER BY id
