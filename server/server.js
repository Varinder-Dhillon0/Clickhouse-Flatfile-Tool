const express = require("express");
const cors = require("cors");
const { createClient } = require("@clickhouse/client");
const multer = require("multer");
const { parse } = require("csv-parse");
const fs = require("fs");
const path = require("path");

const app = express();
const upload = multer({ dest: "uploads/" });

// Enable CORS with more detailed configuration
app.use(cors({
    origin: ["http://localhost:5173", "http://localhost:3000"],
    methods: ['GET', 'POST'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
}));

// Add express json middleware
app.use(express.json());

// Add request logging middleware
app.use((req, res, next) => {
    console.log(`${req.method} ${req.url}`);
    console.log("Request headers:", req.headers);
    console.log("Request body:", req.body);
    next();
});

// Define standard column names for UK property price data
const UK_PROPERTY_COLUMNS = [
    "transaction_id",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
];

// Add these constants at the top of the file
const BATCH_SIZE = 1000;
const DEFAULT_PAGE_SIZE = 100;
const MAX_PAGE_SIZE = 1000;

// Add this function to handle ClickHouse data types
const mapClickHouseType = (type) => {
    const typeMap = {
        'UInt8': 'UInt8',
        'UInt16': 'UInt16',
        'UInt32': 'UInt32',
        'UInt64': 'UInt64',
        'Int8': 'Int8',
        'Int16': 'Int16',
        'Int32': 'Int32',
        'Int64': 'Int64',
        'Float32': 'Float32',
        'Float64': 'Float64',
        'String': 'String',
        'FixedString': 'FixedString',
        'Date': 'Date',
        'DateTime': 'DateTime',
        'DateTime64': 'DateTime64',
        'Array': 'Array',
        'Tuple': 'Tuple',
        'Nullable': 'Nullable',
        'LowCardinality': 'LowCardinality',
        'Enum8': 'Enum8',
        'Enum16': 'Enum16',
        'UUID': 'UUID',
        'IPv4': 'IPv4',
        'IPv6': 'IPv6'
    };
    return typeMap[type] || 'String';
};

// Test endpoint
app.get("/", (req, res) => {
    res.json({ message: "ClickHouse-FlatFile Ingestion Tool Backend" });
});

// Connect to ClickHouse and list tables
app.post("/connect", async (req, res) => {
    console.log("Connect endpoint called with body:", req.body);
    const {
        source,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    if (!req.body || !source) {
        return res.status(400).json({
            success: false,
            error: "Missing required parameters. Request body: " + JSON.stringify(req.body),
        });
    }

    if (source === "ClickHouse") {
        try {
            console.log("Connecting to ClickHouse with:", {
                host: `http://${host}:${port}`,
                username: user,
                database,
            });

            const client = createClient({
                host: `http://${host}:${port}`,
                username: user,
                password: jwtToken,
                database,
            });

            // Test the connection first
            try {
                await client.ping();
            } catch (pingError) {
                console.error("ClickHouse ping failed:", pingError);
                await client.close();
                return res.status(400).json({
                    success: false,
                    error: "Failed to connect to ClickHouse: " + pingError.message,
                });
            }

            // Check if tables exist
            const result = await client.query({
                query: "SHOW TABLES",
                format: "JSONEachRow",
            });

            let tables = await result.json();
            
            // If no tables exist, create a sample table
            if (tables.length === 0) {
                console.log("No tables found, creating a sample table");
                
                // Create a sample table
                await client.query({
                    query: `CREATE TABLE IF NOT EXISTS sample_data (
                        id UInt32,
                        name String,
                        age UInt8,
                        email String,
                        created_at DateTime
                    ) ENGINE = MergeTree()
                    ORDER BY id`,
                });
                
                // Insert sample data
                await client.query({
                    query: `INSERT INTO sample_data (id, name, age, email, created_at) VALUES
                        (1, 'John Doe', 30, 'john@example.com', now()),
                        (2, 'Jane Smith', 25, 'jane@example.com', now()),
                        (3, 'Bob Johnson', 40, 'bob@example.com', now()),
                        (4, 'Alice Brown', 35, 'alice@example.com', now()),
                        (5, 'Charlie Wilson', 28, 'charlie@example.com', now())`,
                });
                
                // Get the updated list of tables
                const updatedResult = await client.query({
                    query: "SHOW TABLES",
                    format: "JSONEachRow",
                });
                
                tables = await updatedResult.json();
            }
            
            await client.close();

            console.log("Successfully connected to ClickHouse, found tables:", tables);
            res.json({ success: true, tables: tables.map((t) => t.name) });
        } catch (error) {
            console.error("ClickHouse connection error:", error);
            res.status(400).json({
                success: false,
                error: error.message,
                stack: error.stack,
            });
        }
    } else {
        res.json({ success: true, message: "Flat File connection ready" });
    }
});

// Get columns for a table or Flat File
app.post("/columns", upload.single("file"), async (req, res) => {
    console.log("Columns endpoint called with body:", req.body);
    const {
        source,
        table,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    if (!req.body || !source) {
        return res.status(400).json({
            success: false,
            error:
                "Missing required parameters. Request body: " +
                JSON.stringify(req.body),
        });
    }

    if (source === "ClickHouse") {
        try {
            console.log("Fetching columns from ClickHouse table:", table);
            const client = createClient({
                host: `http://${host}:${port}`,
                username: user,
                password: jwtToken,
                database,
            });

            const result = await client.query({
                query: `DESCRIBE TABLE ${table}`,
                format: "JSONEachRow",
            });

            const columns = await result.json();
            await client.close();

            console.log("Successfully fetched columns:", columns);
            // Return column objects with name and type information
            res.json({
                success: true,
                columns: columns.map((c) => ({
                    name: c.name,
                    type: c.type,
                })),
            });
        } catch (error) {
            console.error("Error fetching columns from ClickHouse:", error);
            res.status(400).json({
                success: false,
                error: error.message,
                stack: error.stack,
            });
        }
    } else if (req.file) {
        console.log("Parsing uploaded file for columns:", req.file.path);

        // Check file extension
        const fileExt = path.extname(req.file.originalname).toLowerCase();

        if (fileExt === ".csv" || fileExt === ".txt") {
            // For UK property price data files, use predefined column names
            if (
                req.file.originalname.toLowerCase().includes("pp-") ||
                req.file.originalname.toLowerCase().includes("price-paid")
            ) {
                // For UK property price data, use standard column names
                console.log(
                    "Detected UK property price data file, using standard column names"
                );
                res.json({
                    success: true,
                    columns: UK_PROPERTY_COLUMNS.map((name) => ({
                        name,
                        type: "String",
                    })),
                    filePath: req.file.path,
                });
            } else {
                // For other CSV files, attempt to read the header row
                let firstLine = "";
                let hasReadFirstLine = false;

                fs.createReadStream(req.file.path)
                    .on("data", (chunk) => {
                        if (!hasReadFirstLine) {
                            // Find the first newline to get just the header
                            const newlineIndex = chunk.indexOf("\n");
                            if (newlineIndex !== -1) {
                                firstLine += chunk
                                    .slice(0, newlineIndex)
                                    .toString();
                                hasReadFirstLine = true;
                            } else {
                                firstLine += chunk.toString();
                            }
                        }
                    })
                    .on("end", () => {
                        if (firstLine) {
                            // Split the header line by comma to get column names
                            const columnNames = firstLine
                                .split(",")
                                .map((name) => name.trim());
                            console.log(
                                "Successfully parsed columns from header:",
                                columnNames
                            );
                            res.json({
                                success: true,
                                columns: columnNames.map((name) => ({
                                    name,
                                    type: "String",
                                })),
                                filePath: req.file.path,
                            });
                        } else {
                            // Fallback to simple column numbering if header can't be parsed
                            console.log(
                                "Could not parse header, using generic column names"
                            );
                            const parser = parse({ delimiter: "," });
                            let columnCount = 0;

                            parser.on("readable", function () {
                                let record;
                                while ((record = parser.read())) {
                                    columnCount = record.length;
                                    break; // Only need the first row to count columns
                                }
                            });

                            parser.on("end", function () {
                                const columns = Array.from(
                                    { length: columnCount },
                                    (_, i) => ({
                                        name: `column_${i + 1}`,
                                        type: "String",
                                    })
                                );

                                res.json({
                                    success: true,
                                    columns,
                                    filePath: req.file.path,
                                });
                            });

                            fs.createReadStream(req.file.path).pipe(parser);
                        }
                    })
                    .on("error", (error) => {
                        console.error("Error reading file header:", error);
                        res.status(400).json({
                            success: false,
                            error: error.message,
                        });
                    });
            }
        } else {
            res.status(400).json({
                success: false,
                error: "Unsupported file format. Please upload a CSV or TXT file.",
            });
        }
    } else {
        console.error("No file uploaded for parsing columns");
        res.status(400).json({ success: false, error: "No file uploaded" });
    }
});

// Download table data as CSV
app.post("/download", async (req, res) => {
    console.log("Download endpoint called with body:", req.body);
    const {
        tableName,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    if (!tableName) {
        return res.status(400).json({
            success: false,
            error: "Missing table name for download",
        });
    }

    try {
        // Create ClickHouse client
        const client = createClient({
            host: `http://${host}:${port}`,
            username: user,
            password: jwtToken,
            database,
        });

        // Query data from the table
        const result = await client.query({
            query: `SELECT * FROM ${tableName}`,
            format: "CSVWithNames",
        });

        const csvData = await result.text();
        await client.close();

        // Send CSV data
        res.setHeader("Content-Type", "text/csv");
        res.setHeader(
            "Content-Disposition",
            `attachment; filename="${tableName}.csv"`
        );
        res.send(csvData);
    } catch (error) {
        console.error("Error downloading data:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

// Preview data endpoint
app.post("/preview", upload.single("file"), async (req, res) => {
    const {
        source,
        table,
        columns,
        page = 1,
        pageSize = DEFAULT_PAGE_SIZE,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    try {
        let data = [];
        let totalCount = 0;

        if (source === "ClickHouse") {
            const client = createClient({
                host: `http://${host}:${port}`,
                username: user,
                password: jwtToken,
                database,
            });

            const selectedColumns = JSON.parse(columns);
            const columnNames = selectedColumns.map((col) => col.name);
            
            // Get total count
            const countResult = await client.query({
                query: `SELECT count() as total FROM ${table}`,
                format: "JSONEachRow",
            });
            const countData = await countResult.json();
            totalCount = countData[0].total;

            // Calculate offset
            const offset = (page - 1) * pageSize;
            const limit = Math.min(pageSize, MAX_PAGE_SIZE);

            const result = await client.query({
                query: `SELECT ${columnNames.join(", ")} FROM ${table} LIMIT ${limit} OFFSET ${offset}`,
                format: "JSONEachRow",
            });

            data = await result.json();
            await client.close();
        } else if (req.file) {
            const selectedColumns = JSON.parse(columns);
            const columnNames = selectedColumns.map((col) => col.name);
            
            const parser = fs
                .createReadStream(req.file.path)
                .pipe(parse({ columns: true, skip_empty_lines: true }));

            let count = 0;
            let skipCount = (page - 1) * pageSize;
            const limit = Math.min(pageSize, MAX_PAGE_SIZE);

            for await (const record of parser) {
                if (count >= limit) break;
                if (skipCount > 0) {
                    skipCount--;
                    continue;
                }
                const filteredRecord = {};
                columnNames.forEach((col) => {
                    filteredRecord[col] = record[col];
                });
                data.push(filteredRecord);
                count++;
            }

            // Count total records
            const countParser = fs
                .createReadStream(req.file.path)
                .pipe(parse({ columns: true, skip_empty_lines: true }));
            
            for await (const record of countParser) {
                totalCount++;
            }
        }

        res.json({
            success: true,
            data,
            pagination: {
                total: totalCount,
                page: parseInt(page),
                pageSize: parseInt(pageSize),
                totalPages: Math.ceil(totalCount / pageSize)
            }
        });
    } catch (error) {
        console.error("Preview error:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

// Update ingestion endpoint to support progress tracking
app.post("/ingest", upload.single("file"), async (req, res) => {
    console.log("Ingest endpoint called with body:", req.body);
    const {
        source,
        table,
        columns,
        targetTable,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    if (!columns || !targetTable) {
        console.error("Missing required parameters:", { columns: !!columns, targetTable: !!targetTable });
        return res.status(400).json({
            success: false,
            error: "Missing required parameters: columns or targetTable",
        });
    }

    try {
        let count = 0;
        if (source === "ClickHouse") {
            console.log("Creating ClickHouse client with config:", {
                host: `http://${host}:${port}`,
                username: user,
                database,
            });

            const client = createClient({
                host: `http://${host}:${port}`,
                username: user,
                password: jwtToken,
                database,
            });

            const selectedColumns = JSON.parse(columns);
            console.log("Parsed selected columns:", selectedColumns);
            
            if (!Array.isArray(selectedColumns) || selectedColumns.length === 0) {
                throw new Error("No columns selected");
            }

            // Validate and map column types
            selectedColumns.forEach(col => {
                if (!col.name || !col.type) {
                    throw new Error(`Invalid column definition: ${JSON.stringify(col)}`);
                }
                col.type = mapClickHouseType(col.type);
            });

            const columnNames = selectedColumns.map((col) => col.name);
            
            // Create target table if it doesn't exist
            const createTableQuery = `CREATE TABLE IF NOT EXISTS ${targetTable} (
                ${selectedColumns.map((col) => `${col.name} ${col.type}`).join(", ")}
            ) ENGINE = MergeTree()
            ORDER BY tuple()`;

            console.log("Creating table with query:", createTableQuery);
            
            try {
                await client.query({
                    query: createTableQuery,
                });
                console.log("Table created successfully");
            } catch (createError) {
                console.error("Error creating table:", createError);
                throw createError;
            }

            // Insert data in batches with progress tracking
            let offset = 0;
            let hasMore = true;
            let totalProcessed = 0;

            while (hasMore) {
                console.log(`Processing batch at offset ${offset}`);
                const selectQuery = `SELECT ${columnNames.join(", ")} FROM ${table} LIMIT ${BATCH_SIZE} OFFSET ${offset}`;
                
                try {
                    const result = await client.query({
                        query: selectQuery,
                        format: "JSONEachRow",
                    });

                    const batch = await result.json();
                    console.log(`Fetched ${batch.length} records`);

                    if (batch.length === 0) {
                        hasMore = false;
                        break;
                    }

                    if (batch.length > 0) {
                        const insertQuery = `INSERT INTO ${targetTable} (${columnNames.join(", ")})`;
                        const values = batch.map(row => 
                            `(${columnNames.map(col => {
                                const value = row[col];
                                if (value === null || value === undefined) return 'NULL';
                                return typeof value === 'string' ? `'${value.replace(/'/g, "''")}'` : value;
                            }).join(", ")})`
                        ).join(", ");

                        await client.query({
                            query: `${insertQuery} VALUES ${values}`,
                        });

                        count += batch.length;
                        totalProcessed += batch.length;
                        offset += BATCH_SIZE;

                        // Send progress update
                        res.write(JSON.stringify({
                            type: 'progress',
                            processed: totalProcessed,
                            total: count
                        }) + '\n');
                    }
                } catch (batchError) {
                    console.error("Error processing batch:", batchError);
                    throw batchError;
                }
            }

            await client.close();
            console.log("ClickHouse client closed");
        } else if (req.file) {
            const selectedColumns = JSON.parse(columns);
            const columnNames = selectedColumns.map((col) => col.name);
            
            const client = createClient({
                host: `http://${host}:${port}`,
                username: user,
                password: jwtToken,
                database,
            });

            // Create target table
            const createTableQuery = `CREATE TABLE IF NOT EXISTS ${targetTable} (
                ${selectedColumns.map((col) => `${col.name} ${col.type}`).join(", ")}
            ) ENGINE = MergeTree()
            ORDER BY tuple()`;

            await client.query({
                query: createTableQuery,
            });

            // Read and insert data in batches
            const parser = fs
                .createReadStream(req.file.path)
                .pipe(parse({ columns: true, skip_empty_lines: true }));

            let batch = [];
            const batchSize = 1000;

            for await (const record of parser) {
                const filteredRecord = {};
                columnNames.forEach((col) => {
                    filteredRecord[col] = record[col];
                });
                batch.push(filteredRecord);

                if (batch.length >= batchSize) {
                    const insertQuery = `INSERT INTO ${targetTable} (${columnNames.join(", ")}) VALUES`;
                    const values = batch.map((row) => `(${columnNames.map((col) => `'${row[col]}'`).join(", ")})`).join(", ");

                    await client.query({
                        query: `${insertQuery} ${values}`,
                    });

                    count += batch.length;
                    batch = [];
                }
            }

            // Insert remaining records
            if (batch.length > 0) {
                const insertQuery = `INSERT INTO ${targetTable} (${columnNames.join(", ")}) VALUES`;
                const values = batch.map((row) => `(${columnNames.map((col) => `'${row[col]}'`).join(", ")})`).join(", ");

                await client.query({
                    query: `${insertQuery} ${values}`,
                });

                count += batch.length;
            }

            await client.close();
        }

        res.json({
            success: true,
            count,
            message: `Successfully ingested ${count} records`,
        });
    } catch (error) {
        console.error("Ingestion error:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

// Get joinable tables
app.post("/joinable-tables", async (req, res) => {
    const {
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    try {
        const client = createClient({
            host: `http://${host}:${port}`,
            username: user,
            password: jwtToken,
            database,
        });

        const result = await client.query({
            query: "SHOW TABLES",
            format: "JSONEachRow",
        });

        const tables = await result.json();
        await client.close();

        res.json({ success: true, tables: tables.map((t) => t.name) });
    } catch (error) {
        console.error("Error fetching joinable tables:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

// Get join columns for tables
app.post("/join-columns", async (req, res) => {
    const {
        tables,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    try {
        const client = createClient({
            host: `http://${host}:${port}`,
            username: user,
            password: jwtToken,
            database,
        });

        const tableColumns = {};
        for (const table of tables) {
            const result = await client.query({
                query: `DESCRIBE TABLE ${table}`,
                format: "JSONEachRow",
            });
            const columns = await result.json();
            tableColumns[table] = columns.map((c) => ({
                name: c.name,
                type: c.type,
            }));
        }
        await client.close();

        res.json({ success: true, tableColumns });
    } catch (error) {
        console.error("Error fetching join columns:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

// Execute join query
app.post("/execute-join", async (req, res) => {
    const {
        tables,
        joinConditions,
        selectedColumns,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    try {
        const client = createClient({
            host: `http://${host}:${port}`,
            username: user,
            password: jwtToken,
            database,
        });

        // Construct JOIN query
        let joinQuery = "SELECT ";
        joinQuery += selectedColumns.join(", ");
        joinQuery += " FROM " + tables[0];

        for (let i = 1; i < tables.length; i++) {
            joinQuery += ` JOIN ${tables[i]} ON ${joinConditions[i - 1]}`;
        }

        const result = await client.query({
            query: joinQuery,
            format: "JSONEachRow",
        });

        const data = await result.json();
        await client.close();

        res.json({
            success: true,
            data,
            count: data.length,
        });
    } catch (error) {
        console.error("Error executing join query:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

const PORT = process.env.PORT || 8000;

const startServer = (port) => {
    const server = app.listen(port, () => {
        console.log(`Server running on http://localhost:${port}`);
    });

    server.on('error', (error) => {
        if (error.code === 'EADDRINUSE') {
            console.log(`Port ${port} is busy, trying ${port + 1}...`);
            startServer(port + 1);
        } else {
            console.error('Server error:', error);
        }
    });

    return server;
};

startServer(PORT);
