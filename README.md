# VFrames

A high-performance DataFrame library for V, powered by DuckDB.

## Overview

VFrames provides a Pandas-like interface for data manipulation in V, leveraging DuckDB's powerful SQL engine for fast in-memory analytics. Designed for developers who want the expressiveness of Pandas with the performance of a compiled language.

## Features

- **Pandas-like API** - Familiar syntax for data scientists coming from Python
- **DuckDB Backend** - Leverages DuckDB's vectorized execution for exceptional performance
- **Multiple Data Sources** - Read CSV, JSON, and Parquet files with automatic type inference
- **Immutable Design** - All operations return new DataFrames, preventing accidental mutations
- **Rich Functionality** - Support for filtering, grouping, aggregations, and data transformations

## Installation

```bash
# Install dependencies
v install https://github.com/rodabt/vduckdb
v install https://github.com/rodabt/vframes
```

## Quick Start

```v
import vframes

fn main() {
    // Initialize context (in-memory or persisted)
    mut ctx := vframes.init()
    
    // Load data from file
    df := ctx.read_auto('data.csv')!
    
    // Explore data
    df.head(5)
    df.shape()
    df.describe()
    
    ctx.close()
}
```

## Requirements

- V (Vlang) compiler
- DuckDB library (`LIBDUCKDB_DIR` environment variable must be set)

## Documentation

- [Tutorial](TUTORIAL.md) - Step-by-step guide with Pandas comparisons
- [API Reference](IMPLEMENTATION_ROADMAP.md) - Complete function list
- [Examples](examples/) - Real-world usage examples

## Why VFrames?

| Feature | VFrames | Pandas |
|---------|---------|--------|
| Language | V (compiled) | Python (interpreted) |
| Performance | ~10-100x faster | Baseline |
| Memory | Efficient (DuckDB) | RAM-intensive |
| Type Safety | Compile-time | Runtime |

## Contributing

Contributions are welcome! Please read our [contributing guidelines](CODE_OF_CONDUCT.md) before submitting PRs.

## License

MIT License - see [LICENSE](LICENSE) for details.
