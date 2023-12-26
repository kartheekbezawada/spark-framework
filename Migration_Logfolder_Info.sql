CREATE TABLE BlobFolderLogs (
    folder_path NVARCHAR(255),
    num_files INT,
    total_rows INT,
    total_size_gb FLOAT,
    timestamp DATETIME
);