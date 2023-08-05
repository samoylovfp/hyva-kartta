CREATE TABLE nodes (
    id Int64 CODEC(Delta, Default),
    cell3 UInt64 CODEC(Delta, Default),
    cell12 UInt64 CODEC(Delta, Default),
    decimicro_lat Int32 CODEC(Delta, Default),
    decimicro_lon Int32 CODEC(Delta, Default),
    tags Map(UInt64, UInt64)

) ENGINE = ReplacingMergeTree()
ORDER BY (cell12, id)
PARTITION BY cell3

CREATE TABLE StringTable (
    id UInt64,
    string String
) ENGINE = ReplacingMergeTree()
ORDER BY id

