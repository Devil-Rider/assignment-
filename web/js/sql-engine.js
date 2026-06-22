/* ============================================================
   sql-engine.js
   Wraps sql.js (SQLite compiled to WebAssembly) so SQL runs
   entirely in the user's browser — no server required.
   Also seeds a sample "shop" database used by both the public
   SQL Editor and the gamified Learn course.
   ============================================================ */
window.SQLEngine = (function () {
  let SQL = null;       // the sql.js module
  let db = null;        // the active database
  let readyPromise = null;

  // Schema definition (also used to render the schema sidebar)
  const SCHEMA = [
    { name: 'customers', columns: ['id PK', 'name', 'city', 'country', 'signup_date'] },
    { name: 'products',  columns: ['id PK', 'name', 'category', 'price'] },
    { name: 'orders',    columns: ['id PK', 'customer_id FK', 'order_date', 'status'] },
    { name: 'order_items', columns: ['id PK', 'order_id FK', 'product_id FK', 'quantity'] },
    { name: 'employees', columns: ['id PK', 'name', 'department', 'salary', 'manager_id', 'hire_date'] },
  ];

  const SEED_SQL = `
    CREATE TABLE customers (
      id INTEGER PRIMARY KEY, name TEXT, city TEXT, country TEXT, signup_date TEXT
    );
    INSERT INTO customers VALUES
      (1,'Aarav Sharma','Mumbai','India','2023-01-12'),
      (2,'Priya Patel','Ahmedabad','India','2023-02-03'),
      (3,'John Smith','London','UK','2023-02-20'),
      (4,'Mei Lin','Singapore','Singapore','2023-03-15'),
      (5,'Carlos Ruiz','Madrid','Spain','2023-04-01'),
      (6,'Sara Khan','Delhi','India','2023-04-22'),
      (7,'Tom Brown','New York','USA','2023-05-10'),
      (8,'Yuki Tanaka','Tokyo','Japan','2023-06-05'),
      (9,'Ravi Kumar','Bangalore','India','2023-06-18'),
      (10,'Emma Wilson','Sydney','Australia','2023-07-02');

    CREATE TABLE products (
      id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL
    );
    INSERT INTO products VALUES
      (1,'Wireless Mouse','Electronics',24.99),
      (2,'Mechanical Keyboard','Electronics',89.50),
      (3,'Coffee Mug','Home',12.00),
      (4,'Notebook','Stationery',5.50),
      (5,'USB-C Cable','Electronics',9.99),
      (6,'Desk Lamp','Home',34.00),
      (7,'Water Bottle','Home',18.75),
      (8,'Gel Pen Pack','Stationery',3.25),
      (9,'Monitor Stand','Electronics',45.00),
      (10,'Backpack','Travel',59.90);

    CREATE TABLE orders (
      id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT, status TEXT
    );
    INSERT INTO orders VALUES
      (101,1,'2023-08-01','delivered'),
      (102,2,'2023-08-02','delivered'),
      (103,1,'2023-08-05','shipped'),
      (104,3,'2023-08-07','delivered'),
      (105,4,'2023-08-09','cancelled'),
      (106,6,'2023-08-11','delivered'),
      (107,7,'2023-08-14','shipped'),
      (108,9,'2023-08-15','delivered'),
      (109,2,'2023-08-18','pending'),
      (110,1,'2023-08-20','delivered'),
      (111,8,'2023-08-22','delivered'),
      (112,10,'2023-08-25','shipped');

    CREATE TABLE order_items (
      id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER
    );
    INSERT INTO order_items VALUES
      (1,101,1,2),(2,101,5,1),(3,102,2,1),(4,103,3,4),(5,104,6,1),
      (6,104,7,2),(7,105,10,1),(8,106,4,5),(9,106,8,3),(10,107,2,1),
      (11,107,9,1),(12,108,1,1),(13,108,5,2),(14,109,3,2),(15,110,6,1),
      (16,110,7,1),(17,110,1,1),(18,111,10,1),(19,112,9,2),(20,112,2,1);

    CREATE TABLE employees (
      id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL,
      manager_id INTEGER, hire_date TEXT
    );
    INSERT INTO employees VALUES
      (1,'Nina Rao','Management',150000,NULL,'2020-01-15'),
      (2,'Alex Carter','Engineering',120000,1,'2020-03-01'),
      (3,'Bilal Ahmed','Engineering',95000,2,'2021-06-10'),
      (4,'Grace Lee','Engineering',98000,2,'2021-07-22'),
      (5,'Omar Faruk','Sales',85000,1,'2020-09-05'),
      (6,'Hana Kim','Sales',72000,5,'2022-02-18'),
      (7,'Diego Mora','Sales',69000,5,'2022-03-30'),
      (8,'Lily Chen','Marketing',78000,1,'2021-11-12'),
      (9,'Sam Patel','Marketing',64000,8,'2022-08-01');
  `;

  function init() {
    if (readyPromise) return readyPromise;
    readyPromise = (async () => {
      if (typeof initSqlJs !== 'function') {
        throw new Error('sql.js failed to load. Check your internet connection (the SQL engine is loaded from a CDN).');
      }
      SQL = await initSqlJs({
        locateFile: (file) =>
          `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.10.3/${file}`,
      });
      reset();
      return true;
    })();
    return readyPromise;
  }

  // (Re)create a fresh database with seed data.
  function reset() {
    if (db) db.close();
    db = new SQL.Database();
    db.run(SEED_SQL);
  }

  /* Run SQL. Returns { results: [{columns, values}], elapsedMs }.
     Throws on SQL error. */
  function run(sql) {
    const t0 = performance.now();
    const results = db.exec(sql);
    const elapsedMs = +(performance.now() - t0).toFixed(1);
    return { results, elapsedMs };
  }

  function getSchema() { return SCHEMA; }

  return { init, run, reset, getSchema };
})();
