### TODO
- Implement PID-based stale lock detection

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
SELECT * FROM users;
```
