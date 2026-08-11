# HozonDB

A relational database engine built from scratch in Rust. The goal is to understand how databases work at the implementation level — storage, indexing, crash recovery, and eventually distributed consensus — by building each piece rather than using existing libraries.

---

## Architecture

![HozonDB Architecture](assets/architecture.png)

**Crate layout:**

| Crate | Role |
|---|---|
| `core` | Storage engine, SQL executor, WAL, buffer pool |
| `server` | gRPC server — runs the database as a standalone process |
| `client` | gRPC client library — transport layer for connecting to the server |
| `hsql` | readline-powered REPL that connects over gRPC |
| `tests` | Integration tests — recovery, persistence, gRPC |

**Page layout:**
```
Page 0:   file header
Page 1:   table catalog  (schema persistence)
Page 2:   index catalog  (index metadata + root page IDs)
Page 3+:  user data pages and B+ tree node pages (shared space)
```

---

## Storage

HozonDB stores all data in a single `.hdb` file. The file is a sequence of fixed-size 4KB pages. Each page has a type:

- **Slotted pages** — row data. Each page has a slot directory at the front and row data growing from the back. Rows have stable `(page_id, slot)` addresses even after updates.
- **Raw pages** — B+ tree index nodes and system catalog pages.
- **Free pages** — released pages tracked in a linked free list, reused on next allocation.

A separate `.wal` file holds the write-ahead log.

---

## Indexing

Every `PRIMARY KEY` column automatically gets a B+ tree index. The index is stored as raw pages within the same `.hdb` file.

```sql
-- auto-creates a B+ tree index on `id`
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
```

On every `INSERT`, the indexed column value and the row's `RowLocation` (page + slot) are inserted into the tree. On `SELECT WHERE id = 5`, the executor uses the tree to find the exact page and slot in O(log n) rather than scanning all pages.

**Index-eligible operators:**
- `=` — point lookup, reads 1 data page
- `<`, `<=`, `>`, `>=` — range scan, walks the leaf linked list
- All other predicates (`!=`, `AND`, `OR`, compound) — fall back to full scan

---

## Buffer Pool

The buffer pool sits between the executor and disk. Every page read checks the pool first. Every page write goes through it — the change is logged to WAL, the frame is marked dirty, and the actual disk flush is deferred to checkpoint time.

Clock sweep eviction handles memory pressure — referenced frames get a second chance before eviction, the same algorithm PostgreSQL uses.

---

## Write-Ahead Log (WAL)

Every write is logged before it touches a page. HozonDB uses physiological logging — records describe changes at the page and slot level, not raw byte offsets or full SQL statements.

WAL record types:

- `Slotted` — row-level DML (INSERT, UPDATE, DELETE): table, page, slot, old bytes, new bytes
- `Raw` — full page image for B+ tree nodes and catalog pages
- `Checkpoint` — recovery boundary marker
- `LinkPage` — page chain pointer change
- `AllocatePage` — page lifecycle
- `Commit` / `Abort` — transaction boundary markers; recovery uses these to decide which transactions' writes to keep versus undo

On startup, `WalReader` replays every record from the last checkpoint forward, regardless of which transaction it belongs to — each record is applied only if the target page's stored LSN is older than the record's LSN, idempotent by design. After redo, any transaction that never wrote a `Commit` record — whether it was explicitly rolled back or the process crashed mid-transaction — is undone using the same page-level `old_data` mechanism a live `ROLLBACK` uses.

CRC32 checksum per record detects torn writes. Recovery stops at the last valid record if corruption is detected.

---

## Transactions

`BEGIN` / `COMMIT` / `ROLLBACK` are fully supported. Every statement runs inside a transaction — implicit and auto-committed if no `BEGIN` is open, explicit otherwise.

```sql
hozondb> BEGIN;
hozondb> INSERT INTO users VALUES (3, 'Charlie');
hozondb> ROLLBACK;
```

- `ROLLBACK` undoes a transaction's writes using the `old_data` captured in each WAL record, walked in reverse.
- `COMMIT` writes a WAL `Commit` record and fsyncs once for the whole transaction — this is what makes group commit possible (see Benchmark Results below).
- Crash recovery distinguishes committed transactions from unresolved ones the same way a live rollback does, so a crash between `BEGIN` and `COMMIT` never gets silently replayed as if it had committed.
- Only one transaction can be open at a time across the whole database — see Known gaps.

---

## Benchmark Results

**10,000 rows, with B+ tree index on primary key**

| Operation | Duration | BP Hits | Pages Dirtied |
|---|---|---|---|
| SELECT full scan | 10.09ms | 66 | — |
| SELECT idx seek (point lookup) | 0.02ms | 1 | — |
| INSERT (single row) | 4.23ms | 1 | 1 |
| UPDATE (fits slot) | 5.06ms | 1 | 1 |
| UPDATE (exceeds slot) | 5.06ms | 2 | 3 |
| UPDATE bulk 10% (1000 rows) | 263.94ms | 1000 | 8 |
| DELETE (single row) | 5.89ms | 1 | 1 |
| DELETE bulk 10% (1000 rows) | 140.16ms | 1000 | 8 |
| INSERT x1000, no transaction | 5,027.92ms | 1000 | 7 |
| INSERT x1000, one explicit transaction | 217.04ms | 1000 | 8 |

Group commit landed with transaction support. Every WAL append used to fsync individually, so writes touching many rows paid one fsync per row — the bulk UPDATE and DELETE numbers above dropped ~46x and ~55x from their pre-transaction baselines (12,227.99ms → 263.94ms, 7,740.22ms → 140.16ms) purely from deferring the fsync to the end of the (implicit, single-statement) transaction.

The cleanest isolated comparison is the last two rows: the same 1,000 `INSERT`s, same code path, the only difference is whether they're wrapped in an explicit transaction. Without one, each insert fsyncs on its own — 5,027.92ms. Wrapped in `BEGIN` / `COMMIT`, it's one fsync for the whole batch — 217.04ms, ~23x faster.

---

## Status / Roadmap

**Implemented:**
- Slotted page storage with stable row addresses
- Page manager with file locking and free list
- B+ tree indexing — point lookup and range scan
- Index-aware INSERT, UPDATE, DELETE
- PRIMARY KEY uniqueness enforcement
- System catalog with schema and index persistence
- Full SQL CRUD with WHERE filtering and range operators
- Buffer pool with clock sweep eviction
- Write-ahead log with physiological logging, CRC32 checksums, and checkpointing
- `BEGIN` / `COMMIT` / `ROLLBACK` transactions, with WAL-based undo on rollback
- Group commit — WAL fsync deferred to transaction commit instead of every write
- Crash recovery via WAL redo followed by undo of any transaction that never committed
- gRPC client-server interface (tonic + tokio)
- `hsql` interactive CLI over gRPC

**Known gaps:**
- Only one transaction can be open at a time across the whole database. The gRPC server also has no session ownership — the mutex is only held for a single RPC, so a second client's statement issued between another client's `BEGIN` and `COMMIT` can silently land inside that open transaction instead of erroring
- No isolation levels — no snapshotting or locking; concurrent access has no formal guarantees beyond the single-active-transaction constraint above
- Dead slot compaction — deleted rows leave dead slots permanently; free space is never reclaimed within a page
- `DROP TABLE` orphans B+ tree index pages — node pages are never freed, only the catalog entry is removed
- Single-page catalog limit — table and index catalogs are each limited to 4KB; overflow returns an error
- SELECT buffers all results — no true server-side streaming
- Index seek for UPDATE/DELETE WHERE — falls back to full scan on indexed columns; correct results, performance gap only
- B+ tree in-memory node cache grows unbounded within a session
- WAL truncation not implemented — `.wal` file grows unbounded; old records before the last checkpoint are never deleted
- `pin_count` in `Frame` exists but is never enforced — safe now (single-threaded), gap when concurrency arrives

**Planned:**
- Concurrent transactions — real isolation levels, and session ownership on the gRPC server (currently one global transaction slot for the whole database)
- `CREATE INDEX` — explicit index creation on any column
- Distributed replication (Raft consensus)

---

## Quick Start

**Start the server:**
```bash
cargo run -p hozondb-server -- mydb
```

Optionally specify a custom address (default: `[::]:50051`):
```bash
cargo run -p hozondb-server -- mydb --addr 0.0.0.0:50051
```

**Connect with the CLI:**
```bash
cargo run -p hsql -- http://localhost:50051
```

```sql
hozondb> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
hozondb> INSERT INTO users VALUES (1, 'Alice');
hozondb> INSERT INTO users VALUES (2, 'Bob');
hozondb> SELECT * FROM users WHERE id = 1;
hozondb> SELECT * FROM users WHERE id > 1;
hozondb> UPDATE users SET name = 'Alice Smith' WHERE id = 1;
hozondb> DELETE FROM users WHERE id = 2;
hozondb> .exit
```

**Run the benchmark suite:**
```bash
cargo run -p hozondb-core --bin benchmark
```

Optionally pass a custom row count (default: 10,000):
```bash
cargo run -p hozondb-core --bin benchmark -- 50000
```

**Run all tests:**
```bash
cargo test --workspace
```