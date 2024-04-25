CREATE TABLE Migration_Log_Table (
    table_name VARCHAR(255),
    blob_path VARCHAR(255),
    row_count INT,
    validation_status VARCHAR(50),
    timestamp DATETIME,
    Read_start_time DATETIME,
    Read_end_time DATETIME,
    Read_status VARCHAR(50),
    Read_error_message VARCHAR(255),
    Write_start_time DATETIME,
    Write_end_time DATETIME,
    Write_status VARCHAR(50),
    Write_error_message VARCHAR(255)
);
