-- Create a sample table
CREATE TABLE IF NOT EXISTS sample_data (
    id UInt32,
    name String,
    age UInt8,
    email String,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY id;

-- Insert some sample data
INSERT INTO sample_data (id, name, age, email, created_at) VALUES
(1, 'John Doe', 30, 'john@example.com', now()),
(2, 'Jane Smith', 25, 'jane@example.com', now()),
(3, 'Bob Johnson', 40, 'bob@example.com', now()),
(4, 'Alice Brown', 35, 'alice@example.com', now()),
(5, 'Charlie Wilson', 28, 'charlie@example.com', now()); 