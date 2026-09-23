# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

This is a Rust workspace containing multiple crates for importing Helium network data from S3 into DuckDB:

- **huckli-import**: Main CLI application and data processing logic
- **huckli-import-derive**: Procedural macros for code generation
- **huckli-db**: DuckDB database abstraction layer
- **huckli-s3**: AWS S3 client wrapper for streaming compressed protobuf files

The system processes various Helium network data types (IoT rewards, mobile rewards, coverage objects, speedtests, etc.) by:
1. Listing files from S3 buckets with time-based filtering
2. Streaming and decompressing gzipped protobuf files
3. Decoding protobuf messages using helium-proto
4. Converting to internal data structures using From trait implementations
5. Bulk inserting into DuckDB tables

## Development Commands

### Building
```bash
cargo build           # Build all workspace members
cargo build --release # Release build
```

### Testing
```bash
cargo test            # Run all tests
```

### Code Quality
```bash
cargo fmt             # Format code
cargo clippy          # Run linter
```

### Running the Import Tool
The main binary is in huckli-import:
```bash
cargo run -- --db <database_path> --file-type <SupportedFileTypes> [S3_OPTIONS] [TIME_OPTIONS]
```

Time options:
- `--after <datetime>`: Import files after this timestamp
- `--before <datetime>`: Import files before this timestamp  
- `--continue`: Resume from last processed file (mutually exclusive with --after)

## Key Patterns

- All data types implement the `DbTable` trait for database operations
- The `get_and_persist` function provides the common pattern for S3->DB import
- Protobuf decoding uses `prost::Message` with error handling for malformed records
- Time handling supports both second and millisecond timestamps with automatic detection
- Public keys are encoded as base58check strings with version byte
- Database schema creation is handled per data type with CREATE TABLE IF NOT EXISTS