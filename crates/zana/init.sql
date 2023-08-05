CREATE TABLE IF NOT EXISTS nodes (
    id INT PRIMARY KEY,

    decimicro_lat INTEGER NOT NULL,
    decimicro_lon INTEGER NOT NULL,

    cell12 INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS paths (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS path_nodes (path_id INTEGER, node_id INTEGER, PRIMARY KEY (path_id, node_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS path_tags (path_id INTEGER, key INTEGER, value INTEGER);
CREATE TABLE IF NOT EXISTS node_tags (node_id INTEGER, tag_id INTEGER, value INTEGER);
CREATE TABLE IF NOT EXISTS strings (s TEXT PRIMARY KEY, id INTEGER) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS path_to_nodes ON path_nodes (path_id);
CREATE INDEX IF NOT EXISTS path_to_tags ON path_tags (path_id);
CREATE INDEX IF NOT EXISTS node_to_tags ON node_tags (node_id);
