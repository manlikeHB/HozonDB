### TODO
- Implement PID-based stale lock detection
- Handle when a page is full
- Better error handling?
- Documentation
- Reclaim pages when tables are dropped 

### Concerns
- Should new created db files be automatically added to `.gitignore`?

### sql statement
```javascript
CREATE TABLE users (id INTEGER);
INSERT INTO users VALUES (42);
SELECT * FROM users;
```

```javascript
CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN);
INSERT INTO users VALUES (1, 'Alice', true);
INSERT INTO users VALUES (2, 'Bob', true);
SELECT * FROM users;
```

```javascript
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER, active BOOLEAN);
INSERT INTO users VALUES (1, 'Alice', 25, true);
INSERT INTO users VALUES (2, 'Bob', 30, true);
INSERT INTO users VALUES (3, 'Charlie', 35, false);
INSERT INTO users VALUES (4, 'Diana', 28, true);
```
