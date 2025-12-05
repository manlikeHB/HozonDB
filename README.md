# HozonDB 🗄️

A SQLite-like database built from scratch in Rust 🦀.

## Status

- Phase 1: Page-based storage + file locking  ✅
- Phase 2: SQL support (CREATE TABLE, INSERT, SELECT) ✅
- Phase 3: WHERE clauses (in progress) 🔨
- Phase 4: Query Execution Engine 📅
- Phase 5: Indexing 📅

## Quick Start
```bash
cargo run
```
```sql
hozondb> .open test.hdb
hozondb> CREATE TABLE users (id INTEGER, name TEXT);
hozondb> INSERT INTO users VALUES (1, 'Alice');
hozondb> SELECT * FROM users;
hozondb> .exit
```

## About

Learning project built in public.