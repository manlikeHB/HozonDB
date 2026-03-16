# HozonDB

A relational database engine built from scratch in Rust. HozonDB implements core database internals — page-based storage, a hand-written SQL parser, query execution, and B-tree indexing.

---

## Why This Project Exists

HozonDB is a systems programming project aimed at exploring the internal architecture of relational databases.

Modern databases hide significant complexity behind simple SQL interfaces. This project focuses on implementing core components from scratch in order to understand how storage engines, query execution, and data layout interact in real systems.

Rather than relying on existing database libraries, HozonDB explicitly implements key subsystems such as page management, row serialization, SQL parsing, and query execution. The goal is to make the behavior of the database transparent and observable while experimenting with design trade-offs commonly found in production database engines.

The project prioritizes clarity of implementation and correctness while progressively introducing more advanced features such as indexing, crash recovery, and transaction management.

---

## Capabilities

- Page-based persistent storage with file-level locking
- SQL: `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- `WHERE` clause filtering with comparison operators
- Multi-page tables with automatic page allocation
- Manual compaction to reclaim space after deletions
- System catalog for schema persistence across restarts
- Benchmark suite with baseline metrics (pre-indexing)

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
│            Query Executor                │  AST → plan nodes → row iteration
│   Scan / Filter / Project operators      │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│         Table Storage Layer              │  Schema-aware row serialization
│   Row encoding / decoding, page routing  │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│            Page Manager                  │  Fixed 4KB pages, file I/O, metadata
│   allocate_page / read_page / write_page │
└────────────────────┬─────────────────────┘
                     │
                  .hdb file
```

The catalog (`__tables__`) is a reserved page-backed structure that bootstraps schema persistence. All user table metadata is stored and recovered from this page on open.

---

## Code Structure
```
src/
├── main.rs              # Binary entry point; opens DB and starts REPL
├── lib.rs               # Crate root; re-exports public API
├── repl.rs              # Read-eval-print loop; routes SQL vs dot-commands
│                        #   (.open, .benchmark, .exit, etc.)
│
├── storage/
│   ├── page.rs          # PageManager: fixed 4KB pages, file I/O, locking,
│   │                    #   page metadata (last_offset, is_full, next_page)
│   └── table.rs         # Row serialization/deserialization, page chain
│                        #   traversal, INSERT routing, compaction
│
├── sql/
│   ├── parser.rs        # Hand-written lexer + recursive descent parser;
│   │                    #   produces Statement AST
│   └── executor.rs      # Walks AST; drives scan/filter/project pipeline
│
├── catalog/
│   └── schema.rs        # Schema struct (table name + column defs);
│                        #   serialization to/from page 1
│
├── benchmark/
│   ├── mod.rs           # Re-exports QueryMetrics, BenchmarkRunner
│   ├── runner.rs        # BenchmarkRunner: setup, run_all_benchmarks, cleanup;
│   │                    #   drives SELECT/INSERT/UPDATE/DELETE benchmark cases
│   └── metrics.rs       # QueryMetrics: tracks duration, pages_read,
│                        #   rows_scanned per query
│
tests/
└── integration_tests.rs # End-to-end SQL tests against real .hdb files
```

**Key type relationships:**
- `PageManager` owns the file handle and all raw I/O
- `Table` holds a reference to `PageManager` + a `Schema`; handles row encoding
- `Executor` holds a `Table` reference; drives query logic
- `Catalog` is backed by a reserved page in `PageManager` (page 1)
- `BenchmarkRunner` borrows `Executor` mutably; invoked via `.benchmark` dot-command in the REPL

---

## Storage Model

**Page layout:** Fixed 4096-byte pages. Page 0 is the file header (stores total page count). Page 1 is reserved for the system catalog. User data begins at page 2.

**Page metadata:** Each data page carries an in-band metadata header tracking:
- `last_offset`: byte offset of the next free slot
- `is_full`: flag to skip full pages during insert
- `next_page`: pointer for chained page traversal (multi-page tables)

**Row format:** Variable-length rows. Each column value is prefixed by a type tag (1 byte) followed by the encoded value. `TEXT` fields are length-prefixed. `INTEGER` and `BOOLEAN` are fixed-width. `NULL` is encoded as a single tag byte.

**File locking:** On open, `PageManager` acquires an exclusive `.lock` file to prevent concurrent access. A second process attempting to open the same database receives `ErrorKind::WouldBlock`.

**Compaction:** After `DELETE`, pages may contain gaps. Manual compaction rewrites all live rows into a fresh page sequence and truncates the file, reclaiming dead space.

---

## Query Execution

SQL is processed in two stages:

**Parsing:** A hand-written lexer tokenizes input into keywords, identifiers, literals, and punctuation. A recursive descent parser consumes the token stream and emits an AST (`Statement` enum with variants per SQL command).

**Execution:** The executor walks the AST directly (no separate planner yet). `SELECT` is implemented as a pipeline: full table scan → predicate filter → column projection. `WHERE` predicates are evaluated per-row against deserialized column values.

There is no query optimizer. All reads are full table scans until B-tree indexing is complete.

---

## Example Query Flow

Trace of `SELECT * FROM users WHERE id = 2;` through the system:

```
1. REPL reads input string
   → repl.rs dispatches to SQL handler

2. Lexer tokenizes input
   → [SELECT, Star, FROM, Ident("users"), WHERE, Ident("id"), Eq, Int(2)]

3. Parser consumes token stream
   → Statement::Select {
       table: "users",
       columns: Wildcard,
       predicate: Some(Expr::Eq("id", Value::Integer(2)))
     }

4. Executor receives AST
   → looks up "users" schema from catalog (page 1)
   → resolves table's first data page from catalog

5. Page scan loop
   → PageManager::read_page(page_id) for each page in chain
   → deserializes rows from raw bytes using schema column types
   → evaluates WHERE predicate per row
   → matching rows collected into result set

6. Result formatted and printed
   +----+------+
   | id | name |
   +----+------+
   | 2  | Bob  |
   +----+------+
```

Every `SELECT` currently reads all pages in the table's chain regardless of predicate selectivity. Index-accelerated lookup (O(log n) seek via B-tree) is the next phase.

---

## Engineering Notes

**Bootstrap problem:** The catalog page stores all table schemas, including its own implicit schema. On first open, the catalog is initialized with a hardcoded layout; subsequent opens deserialize it from page 1.

**Page routing:** `INSERT` walks the page chain for the target table, skipping full pages via the `is_full` flag, and appends to the first non-full page or allocates a new one.

**Update cost:** `UPDATE` currently requires a full table rewrite — read all pages, deserialize all rows, modify matching rows, re-serialize, write all pages back. This is the primary performance bottleneck and motivates B-tree indexing.

**Benchmark results (10,000 rows, 59 pages, no indexes):**

| Operation | Cost |
|---|---|
| Full table scan | ~8ms |
| Single row lookup (`WHERE id = N`) | ~8ms (identical — no index) |
| Single `INSERT` | ~5.58ms |
| Single `UPDATE` | ~236ms |
| Bulk `DELETE` (10%) + compact | ~338ms |

Every read touches all 59 pages and deserializes ~293,000 rows regardless of selectivity. This is the expected O(n) baseline before indexing.

---

## Status / Roadmap

**Implemented:**
- Page manager with file locking and metadata
- System catalog with schema persistence
- Full SQL CRUD with WHERE filtering
- Multi-page tables and compaction
- Benchmark suite

**In progress:**
- B-tree indexing (in-memory → persistent → query integration)

**Planned:**
- Write-ahead log (WAL) for crash recovery
- `BEGIN` / `COMMIT` / `ROLLBACK` transaction support
- Table-level locking (shared/exclusive)

---

## Quick Start

```bash
cargo run
```

```sql
hozondb> .open mydb.hdb
hozondb> CREATE TABLE users (id INTEGER, name TEXT);
hozondb> INSERT INTO users VALUES (1, 'Alice');
hozondb> SELECT * FROM users WHERE id = 1;
hozondb> .exit
```

```bash
cargo test
```