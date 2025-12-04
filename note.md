### TODO
- Implement PID-based stale lock detection

### Concerns
- Should new created db files be automatically added to `.gitignore`?
- Handle page full



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
