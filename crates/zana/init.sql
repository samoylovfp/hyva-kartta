CREATE TABLE nodes (
    id Int64,
    cell3 UInt64,
    cell12 UInt64,
    decimicro_lat Int32,
    decimicro_lon Int32,
    tags Map(UInt64, UInt64)

) ENGINE = ReplacingMergeTree()
ORDER BY (cell12, id)
PARTITION BY cell3

CREATE TABLE StringTable (
    id UInt64,
    string String
) ENGINE = ReplacingMergeTree()
ORDER BY id

