# ClickHouse FlatFile Ingestion Tool

A web-based tool for ingesting data between ClickHouse databases and flat files (CSV/TXT). This tool provides a user-friendly interface for data migration, preview, and ingestion operations.

## Features

-   Connect to ClickHouse databases
-   Upload and process CSV/TXT files
-   Preview data before ingestion
-   Select specific columns for ingestion
-   Paginated data preview
-   Real-time progress tracking
-   Support for various ClickHouse data types
-   Batch processing for large datasets
-   Download ingested data as CSV

## Prerequisites

-   Node.js (v14 or higher)
-   Docker and Docker Compose
-   ClickHouse server (provided via Docker)

## Project Structure

```
clickhouse-flatfile-tool/
├── client/                 # Frontend React application
│   ├── src/               # Source files
│   ├── public/            # Static files
│   └── package.json       # Frontend dependencies
├── server/                # Backend Node.js server
│   ├── server.js          # Main server file
│   └── package.json       # Backend dependencies
├── clickhouse/            # ClickHouse configuration
│   └── create_sample_table.sql  # Sample table creation script
└── docker-compose.yml     # Docker configuration
```

## Setup Instructions

1. Clone the repository:

    ```bash
    git clone <repository-url>
    cd clickhouse-flatfile-tool
    ```

2. Install dependencies:

    ```bash
    # Install backend dependencies
    cd server
    npm install

    # Install frontend dependencies
    cd ../client
    npm install
    ```

3. Start the ClickHouse container:

    ```bash
    docker-compose up -d
    ```

4. Start the backend server:

    ```bash
    cd server
    npm run dev
    ```

5. Start the frontend development server:
    ```bash
    cd client
    npm run dev
    ```

## Configuration

### ClickHouse Connection

Default connection settings:

-   Host: localhost
-   Port: 8123
-   Database: default
-   User: default
-   Password: (leave empty)

You can modify these settings in the UI or update them in the server configuration.

### File Upload

Supported file formats:

-   CSV (.csv)
-   Text files (.txt)

## Usage Instructions

1. **Connect to ClickHouse**:

    - Select "ClickHouse" as the data source
    - Enter connection details
    - Click "Connect"

2. **Select Source Data**:

    - Choose a table from the dropdown
    - Click "Load Columns"
    - Select the columns you want to ingest

3. **Preview Data**:

    - Click "Preview Data" to see the first page
    - Use pagination controls to navigate
    - Adjust page size as needed

4. **Ingest Data**:

    - Enter a target table name
    - Click "Ingest Data"
    - Monitor progress in the status bar

5. **Download Data**:
    - After successful ingestion, use the download button
    - Data will be exported as CSV

## Development

### Backend Development

The server is built with:

-   Node.js
-   Express
-   @clickhouse/client
-   multer (file upload)
-   csv-parse (CSV parsing)

### Frontend Development

The client is built with:

-   React
-   Tailwind CSS
-   Axios
-   Vite

## Error Handling

The application includes comprehensive error handling for:

-   Connection issues
-   Invalid data types
-   File upload errors
-   Ingestion failures
-   Network errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

-   ClickHouse team for their excellent database
-   React and Node.js communities
-   All contributors to this project
