CREATE TABLE performance_metrics_log (
    timestamp DATETIME,
    operation VARCHAR(255),
    duration FLOAT,
    additional_info VARCHAR(255)
);
Go
CREATE TABLE migration_error_log (
    timestamp DATETIME,
    error VARCHAR(MAX)
);
Go
CREATE TABLE tracking_table (
    folder_path VARCHAR(255),
    processed_date DATETIME
);
Go
CREATE TABLE folder_info_log (
    folder_path VARCHAR(255),
    num_files INT,
    total_rows BIGINT,
    total_size_gb FLOAT,
    timestamp DATETIME
);

