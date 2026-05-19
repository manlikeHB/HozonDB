# HozonDB

A relational database engine built from scratch in Rust. HozonDB implements core database internals — page-based storage, a hand-written SQL parser, query execution, slotted page layout, and B+ tree indexing.

---

## Why This Project Exists

HozonDB is a systems programming project aimed at exploring the internal architecture of relational databases.

Modern databases hide significant complexity behind simple SQL interfaces. This project focuses on implementing core components from scratch in order to understand how storage engines, query execution, and data layout interact in real systems.

Rather than relying on existing database libraries, HozonDB explicitly implements key subsystems such as page management, row serialization, SQL parsing, query execution, and indexing. The goal is to make the behavior of the database transparent and observable while experimenting with design trade-offs commonly found in production database engines.

---

## Capabilities

- Page-based persistent storage with file-level locking
- **Slotted page layout** — stable row locations, in-place updates and deletes
- SQL: `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- `PRIMARY KEY` support with automatic B+ tree index creation
- `WHERE` clause filtering with comparison and range operators (`=`, `<`, `>`, `<=`, `>=`)
- **B+ tree indexing** — O(log n) point lookups and range scans on indexed columns
- Index-aware `INSERT`, `UPDATE`, `DELETE` — indexes stay consistent on every write
- Multi-page tables with automatic page allocation
- System catalog for schema and index persistence across restarts
- Benchmark suite with before/after index metrics

---

## Architecture

```
┌──────────────────────────────────────────┐
│               REPL / CLI                 │  rustyline-based interactive shell
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│              SQL Parser                  │  Hand-written lexer + recursive descent
│   Lexer → Token stream → AST             │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│            Query Executor                │  AST → execution → row iteration
│   Index seek / Range scan / Full scan    │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│           B+ Tree Index Layer            │  Per-column indexes, page-backed nodes
│   Point lookup / Range scan / Node cache │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│         Table Storage Layer              │  Slotted pages, schema-aware row I/O
│   Slot directory, in-place update/delete │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│            Page Manager                  │  Fixed 4KB pages, file I/O, metadata
│   allocate_page / read_page / write_page │
└────────────────────┬─────────────────────┘
                     │
                  .hdb file
```

**Page layout:**
```
Page 0:   file header
Page 1:   table catalog  (schema persistence)
Page 2:   index catalog  (index metadata + root page IDs)
Page 3+:  user data pages and B+ tree node pages (shared space)
```

---

## How Indexing Works

When a table is created with a `PRIMARY KEY` column, HozonDB automatically creates a B+ tree index on that column. The index is stored as a set of pages within the same `.hdb` file.

```sql
-- auto-creates a B+ tree index on `id`
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
```

On every `INSERT`, the indexed column value and the new row's `RowLocation` (page + slot) are inserted into the tree. On `SELECT WHERE id = 5`, the executor uses the tree to find the exact page and slot in O(log n) rather than scanning all pages.

**Index-eligible operators:**
- `=` — point lookup, reads 1 data page
- `<`, `<=`, `>`, `>=` — range scan, walks the leaf linked list
- All other predicates (`!=`, `AND`, `OR`, compound) — fall back to full scan

---

## Benchmark Results

**10,000 rows, 66 pages, with B+ tree index on primary key:**

| Operation | Duration | Pages Read | Rows Scanned |
|---|---|---|---|
| SELECT full scan | 12.27ms | 66 | 10,000 |
| SELECT idx seek (point lookup) | **0.02ms** | **1** | **1** |
| SELECT range scan | 10.81ms | 66 | 10,000 |

| Operation | Duration | Pages Read | Pages Written |
|---|---|---|---|
| INSERT (single row) | 7.34ms | 1 | 1 |
| UPDATE (fits slot) | **9.89ms** | **1** | **1** |
| UPDATE (exceeds slot) | 29.10ms | 2 | 3 |
| DELETE (single row) | **7.68ms** | **1** | **1** |

Point lookups on indexed columns: **59x fewer page reads** compared to full scan.

---

## Status / Roadmap

**Implemented:**
- Page manager with file locking and slotted page layout
- In-place row updates and deletes (no page chain rewrite)
- System catalog with schema and index persistence
- Full SQL CRUD with WHERE filtering and range operators
- B+ tree indexing — point lookup and range scan
- Index-aware INSERT, UPDATE, DELETE
- PRIMARY KEY uniqueness enforcement
- Benchmark suite with index metrics

**Planned:**
- `CREATE INDEX` — explicit index creation on any column
- Write-ahead log (WAL) for crash recovery
- `BEGIN` / `COMMIT` / `ROLLBACK` transaction support
- gRPC interface for client-server access
- Distributed replication (Raft consensus)

---

## Quick Start

```bash
cargo run
```

```sql
hozondb> .open mydb.hdb
hozondb> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
hozondb> INSERT INTO users VALUES (1, 'Alice');
hozondb> INSERT INTO users VALUES (2, 'Bob');
hozondb> SELECT * FROM users WHERE id = 1;
hozondb> SELECT * FROM users WHERE id > 1;
hozondb> UPDATE users SET name = 'Alice Smith' WHERE id = 1;
hozondb> DELETE FROM users WHERE id = 2;
hozondb> .exit
```

```bash
cargo test        # run all tests
cargo run -- .benchmark 10000  # run benchmark with 10,000 rows
```