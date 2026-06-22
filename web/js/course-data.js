/* ============================================================
   course-data.js — Gamified SQL curriculum (beginner → advanced)
   Each lesson runs against the in-browser sample "shop" DB.
   Completion is validated by comparing the learner's result set
   to the result of `solution` (order-insensitive unless
   `ordered: true`).
   ============================================================ */
window.COURSE = [
  /* ---------------- MODULE 1 ---------------- */
  {
    id: 'm1', icon: '🌱', title: 'SQL Basics',
    subtitle: 'SELECT, columns, and reading data',
    lessons: [
      {
        id: 'm1l1', title: 'Your First SELECT', tag: 'SELECT', xp: 20,
        theory: `<p><code>SELECT</code> is how you read data. Use <code>*</code> to fetch every column from a table.</p>
                 <pre>SELECT * FROM table_name;</pre>
                 <p>Our database has a <code>customers</code> table. Let's look at everyone in it.</p>`,
        task: 'Select <b>all columns</b> for <b>all rows</b> from the <code>customers</code> table.',
        starter: 'SELECT * FROM customers;',
        solution: 'SELECT * FROM customers;',
      },
      {
        id: 'm1l2', title: 'Pick Your Columns', tag: 'SELECT cols', xp: 20,
        theory: `<p>Instead of <code>*</code>, list the columns you want, separated by commas. This is cleaner and faster.</p>
                 <pre>SELECT name, city FROM customers;</pre>`,
        task: 'Select only the <code>name</code> and <code>country</code> columns from <code>customers</code>.',
        starter: '-- List the two columns you want, separated by a comma\nSELECT  FROM customers;',
        solution: 'SELECT name, country FROM customers;',
      },
      {
        id: 'm1l3', title: 'Filter with WHERE', tag: 'WHERE', xp: 25,
        theory: `<p><code>WHERE</code> keeps only the rows that match a condition.</p>
                 <pre>SELECT name FROM customers
WHERE country = 'India';</pre>
                 <p>Text values go in single quotes.</p>`,
        task: 'Select the <code>name</code> and <code>city</code> of customers where <code>country</code> is <code>India</code>.',
        starter: "-- Add a WHERE clause to keep only Indian customers\nSELECT name, city FROM customers;",
        solution: "SELECT name, city FROM customers WHERE country = 'India';",
      },
      {
        id: 'm1l4', title: 'Sort with ORDER BY', tag: 'ORDER BY', xp: 25, ordered: true,
        theory: `<p><code>ORDER BY</code> sorts the result. Add <code>DESC</code> for descending.</p>
                 <pre>SELECT name, price FROM products
ORDER BY price DESC;</pre>`,
        task: 'Select <code>name</code> and <code>price</code> from <code>products</code>, ordered by <code>price</code> from highest to lowest.',
        starter: '-- Sort highest price first with ORDER BY ... DESC\nSELECT name, price FROM products;',
        solution: 'SELECT name, price FROM products ORDER BY price DESC;',
      },
      {
        id: 'm1l5', title: 'Limit the Rows', tag: 'LIMIT', xp: 25, ordered: true,
        theory: `<p><code>LIMIT</code> caps how many rows come back — great for "top N" lists.</p>
                 <pre>SELECT name FROM products
ORDER BY price DESC
LIMIT 3;</pre>`,
        task: 'Find the <b>3 cheapest</b> products: select <code>name</code> and <code>price</code>, ordered by price ascending, limited to 3.',
        starter: '-- Order by price ascending, then LIMIT to 3 rows\nSELECT name, price FROM products;',
        solution: 'SELECT name, price FROM products ORDER BY price ASC LIMIT 3;',
      },
    ],
  },

  /* ---------------- MODULE 2 ---------------- */
  {
    id: 'm2', icon: '🔍', title: 'Filtering & Functions',
    subtitle: 'Sharper conditions and calculations',
    lessons: [
      {
        id: 'm2l1', title: 'DISTINCT values', tag: 'DISTINCT', xp: 25,
        theory: `<p><code>DISTINCT</code> removes duplicate rows from the result.</p>
                 <pre>SELECT DISTINCT category FROM products;</pre>`,
        task: 'List each unique <code>category</code> in the <code>products</code> table (no duplicates).',
        starter: '-- Use DISTINCT to drop duplicate categories\nSELECT category FROM products;',
        solution: 'SELECT DISTINCT category FROM products;',
      },
      {
        id: 'm2l2', title: 'AND, OR & comparisons', tag: 'AND / OR', xp: 30,
        theory: `<p>Combine conditions with <code>AND</code> / <code>OR</code> and compare with
                 <code>=</code>, <code>&gt;</code>, <code>&lt;</code>, <code>&gt;=</code>, <code>&lt;=</code>, <code>&lt;&gt;</code>.</p>
                 <pre>SELECT name FROM products
WHERE category = 'Electronics' AND price &lt; 50;</pre>`,
        task: 'Select <code>name</code> and <code>price</code> of <code>Electronics</code> products priced <b>under 50</b>.',
        starter: "-- Combine two conditions with AND\nSELECT name, price FROM products\nWHERE ;",
        solution: "SELECT name, price FROM products WHERE category = 'Electronics' AND price < 50;",
      },
      {
        id: 'm2l3', title: 'IN & BETWEEN', tag: 'IN / BETWEEN', xp: 30,
        theory: `<p><code>IN</code> matches a list; <code>BETWEEN</code> matches an inclusive range.</p>
                 <pre>WHERE country IN ('India','UK')
WHERE price BETWEEN 10 AND 30</pre>`,
        task: 'Select <code>name</code> and <code>country</code> of customers whose <code>country</code> is in (<code>India</code>, <code>USA</code>, <code>Japan</code>).',
        starter: "-- Use WHERE country IN ( ... )\nSELECT name, country FROM customers;",
        solution: "SELECT name, country FROM customers WHERE country IN ('India','USA','Japan');",
      },
      {
        id: 'm2l4', title: 'Pattern match with LIKE', tag: 'LIKE', xp: 30,
        theory: `<p><code>LIKE</code> matches text patterns. <code>%</code> = any characters, <code>_</code> = one character.</p>
                 <pre>WHERE name LIKE 'A%'   -- starts with A
WHERE name LIKE '%a%'  -- contains a</pre>`,
        task: 'Select the <code>name</code> of every product whose name contains the word <code>Cable</code> (use LIKE).',
        starter: "-- Match names containing 'Cable' using LIKE and %\nSELECT name FROM products;",
        solution: "SELECT name FROM products WHERE name LIKE '%Cable%';",
      },
      {
        id: 'm2l5', title: 'Aggregate functions', tag: 'COUNT/AVG', xp: 35,
        theory: `<p>Aggregates summarise many rows into one value: <code>COUNT</code>, <code>SUM</code>, <code>AVG</code>, <code>MIN</code>, <code>MAX</code>.</p>
                 <pre>SELECT COUNT(*) FROM orders;
SELECT AVG(price) FROM products;</pre>`,
        task: 'Return the <b>average price</b> of all products. (One row, one column.)',
        starter: '-- Wrap price in the AVG() aggregate function\nSELECT  FROM products;',
        solution: 'SELECT AVG(price) FROM products;',
      },
    ],
  },

  /* ---------------- MODULE 3 ---------------- */
  {
    id: 'm3', icon: '📊', title: 'Grouping & Aggregation',
    subtitle: 'Summaries per category',
    lessons: [
      {
        id: 'm3l1', title: 'GROUP BY basics', tag: 'GROUP BY', xp: 35,
        theory: `<p><code>GROUP BY</code> collapses rows that share a value, so aggregates run per group.</p>
                 <pre>SELECT category, COUNT(*)
FROM products GROUP BY category;</pre>`,
        task: 'For each <code>category</code> in <code>products</code>, return the category and the <b>number of products</b> (<code>COUNT(*)</code>) in it.',
        starter: '-- Group rows by category, then COUNT(*) each group\nSELECT category, COUNT(*) FROM products;',
        solution: 'SELECT category, COUNT(*) FROM products GROUP BY category;',
      },
      {
        id: 'm3l2', title: 'Aliases with AS', tag: 'AS', xp: 30,
        theory: `<p>Rename output columns with <code>AS</code> for readable results.</p>
                 <pre>SELECT category, COUNT(*) AS product_count
FROM products GROUP BY category;</pre>`,
        task: 'For each <code>country</code> in <code>customers</code>, return the country and a count aliased as <code>total_customers</code>.',
        starter: '-- Alias the count column AS total_customers\nSELECT country, COUNT(*)\nFROM customers GROUP BY country;',
        solution: 'SELECT country, COUNT(*) AS total_customers FROM customers GROUP BY country;',
      },
      {
        id: 'm3l3', title: 'Filter groups with HAVING', tag: 'HAVING', xp: 40,
        theory: `<p><code>WHERE</code> filters rows <i>before</i> grouping; <code>HAVING</code> filters groups <i>after</i>.</p>
                 <pre>SELECT category, COUNT(*) AS c
FROM products GROUP BY category
HAVING COUNT(*) &gt; 2;</pre>`,
        task: 'Return each <code>category</code> and its product count (as <code>c</code>) for categories that have <b>more than 2</b> products.',
        starter: '-- Filter groups with HAVING COUNT(*) > 2\nSELECT category, COUNT(*) AS c\nFROM products GROUP BY category;',
        solution: 'SELECT category, COUNT(*) AS c FROM products GROUP BY category HAVING COUNT(*) > 2;',
      },
      {
        id: 'm3l4', title: 'SUM per group', tag: 'SUM', xp: 40, ordered: true,
        theory: `<p>Combine grouping with <code>SUM</code> to total a measure per group, then sort it.</p>
                 <pre>SELECT status, COUNT(*) AS n
FROM orders GROUP BY status ORDER BY n DESC;</pre>`,
        task: 'Count orders per <code>status</code>: return <code>status</code> and count as <code>n</code>, ordered by <code>n</code> descending.',
        starter: '-- Group by status, count as n, then ORDER BY n DESC\nSELECT status, COUNT(*) AS n\nFROM orders GROUP BY status;',
        solution: 'SELECT status, COUNT(*) AS n FROM orders GROUP BY status ORDER BY n DESC;',
      },
    ],
  },

  /* ---------------- MODULE 4 ---------------- */
  {
    id: 'm4', icon: '🔗', title: 'Joins',
    subtitle: 'Combine data across tables',
    lessons: [
      {
        id: 'm4l1', title: 'INNER JOIN', tag: 'JOIN', xp: 45,
        theory: `<p>A <code>JOIN</code> stitches rows from two tables on a matching key.</p>
                 <pre>SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id;</pre>
                 <p><code>o</code> and <code>c</code> are table aliases.</p>`,
        task: 'Return each order\'s <code>id</code> (from <code>orders</code>) and the customer\'s <code>name</code>, by joining <code>orders</code> to <code>customers</code> on <code>customer_id = customers.id</code>.',
        starter: '-- Complete the ON condition that links the two tables\nSELECT o.id, c.name\nFROM orders o\nJOIN customers c ON ;',
        solution: 'SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id;',
      },
      {
        id: 'm4l2', title: 'Joining three tables', tag: 'multi-join', xp: 50,
        theory: `<p>Chain joins to follow relationships: orders → order_items → products.</p>
                 <pre>FROM orders o
JOIN order_items oi ON oi.order_id = o.id
JOIN products p ON p.id = oi.product_id</pre>`,
        task: 'For each order item, return the order <code>id</code>, the product <code>name</code>, and the <code>quantity</code>. Join <code>orders</code> → <code>order_items</code> → <code>products</code>.',
        starter: '-- Add the second JOIN to bring in products\nSELECT o.id, p.name, oi.quantity\nFROM orders o\nJOIN order_items oi ON oi.order_id = o.id\n-- JOIN products p ON ...\n;',
        solution: 'SELECT o.id, p.name, oi.quantity FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id;',
      },
      {
        id: 'm4l3', title: 'LEFT JOIN', tag: 'LEFT JOIN', xp: 50,
        theory: `<p><code>LEFT JOIN</code> keeps every left-table row even when there's no match (you get <code>NULL</code>s).
                 Useful to find things with <i>no</i> related rows.</p>
                 <pre>FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id</pre>`,
        task: 'List every customer\'s <code>name</code> and the order <code>id</code> (<code>o.id</code>), using a LEFT JOIN from <code>customers</code> to <code>orders</code>. Customers with no orders should still appear.',
        starter: '-- Change this INNER JOIN into a LEFT JOIN\nSELECT c.name, o.id\nFROM customers c\nJOIN orders o ON o.customer_id = c.id;',
        solution: 'SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id;',
      },
      {
        id: 'm4l4', title: 'Aggregate after a join', tag: 'JOIN + GROUP', xp: 55, ordered: true,
        theory: `<p>Join first, then group, to answer "how much per X" questions.</p>
                 <pre>SELECT c.name, COUNT(o.id) AS orders
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.name ORDER BY orders DESC;</pre>`,
        task: 'Return each customer <code>name</code> and their number of orders as <code>orders</code> (use LEFT JOIN so 0-order customers count), ordered by <code>orders</code> descending then <code>name</code> ascending.',
        starter: '-- Group the joined rows and count orders per customer\nSELECT c.name, COUNT(o.id) AS orders\nFROM customers c\nLEFT JOIN orders o ON o.customer_id = c.id\n-- add GROUP BY and ORDER BY here\n;',
        solution: 'SELECT c.name, COUNT(o.id) AS orders FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.name ORDER BY orders DESC, c.name ASC;',
      },
    ],
  },

  /* ---------------- MODULE 5 ---------------- */
  {
    id: 'm5', icon: '🧩', title: 'Subqueries & CTEs',
    subtitle: 'Queries inside queries',
    lessons: [
      {
        id: 'm5l1', title: 'Subquery in WHERE', tag: 'subquery', xp: 55,
        theory: `<p>A subquery is a query nested in another. Here we filter by a computed value.</p>
                 <pre>SELECT name FROM products
WHERE price &gt; (SELECT AVG(price) FROM products);</pre>`,
        task: 'Select the <code>name</code> and <code>price</code> of products priced <b>above the average</b> product price (use a subquery).',
        starter: '-- Replace the ? with a subquery returning the average price\nSELECT name, price FROM products\nWHERE price > ( ? );',
        solution: 'SELECT name, price FROM products WHERE price > (SELECT AVG(price) FROM products);',
      },
      {
        id: 'm5l2', title: 'IN with a subquery', tag: 'IN (subquery)', xp: 60,
        theory: `<p>Feed a subquery's results into <code>IN</code> to filter by membership.</p>
                 <pre>WHERE id IN (SELECT customer_id FROM orders)</pre>`,
        task: 'Select the <code>name</code> of customers who <b>have placed at least one order</b> — i.e. whose <code>id</code> is <code>IN</code> the set of <code>customer_id</code>s found in <code>orders</code>.',
        starter: '-- Fill the IN (...) with a subquery over the orders table\nSELECT name FROM customers\nWHERE id IN ( );',
        solution: 'SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders);',
      },
      {
        id: 'm5l3', title: 'Common Table Expressions', tag: 'WITH / CTE', xp: 65, ordered: true,
        theory: `<p>A <code>WITH</code> clause (CTE) names a temporary result you can reuse — far more readable than nested subqueries.</p>
                 <pre>WITH spend AS (
  SELECT order_id, SUM(quantity) AS items
  FROM order_items GROUP BY order_id
)
SELECT * FROM spend ORDER BY items DESC;</pre>`,
        task: 'Using a CTE named <code>item_counts</code> that computes <code>order_id</code> and total <code>quantity</code> as <code>items</code> per order, select <code>order_id</code> and <code>items</code> ordered by <code>items</code> descending then <code>order_id</code> ascending.',
        starter: 'WITH item_counts AS (\n  SELECT order_id, SUM(quantity) AS items\n  FROM order_items GROUP BY order_id\n)\nSELECT order_id, items FROM item_counts\nORDER BY items DESC, order_id ASC;',
        solution: 'WITH item_counts AS (SELECT order_id, SUM(quantity) AS items FROM order_items GROUP BY order_id) SELECT order_id, items FROM item_counts ORDER BY items DESC, order_id ASC;',
      },
    ],
  },

  /* ---------------- MODULE 6 ---------------- */
  {
    id: 'm6', icon: '🏆', title: 'Advanced: Window Functions',
    subtitle: 'Rankings, running totals & self-joins',
    lessons: [
      {
        id: 'm6l1', title: 'ROW_NUMBER()', tag: 'window', xp: 70, ordered: true,
        theory: `<p>Window functions compute across a set of rows <i>without</i> collapsing them.
                 <code>ROW_NUMBER() OVER (ORDER BY ...)</code> numbers rows.</p>
                 <pre>SELECT name, price,
  ROW_NUMBER() OVER (ORDER BY price DESC) AS rn
FROM products;</pre>`,
        task: 'Select <code>name</code>, <code>price</code>, and a <code>ROW_NUMBER()</code> as <code>rn</code> ordered by <code>price</code> descending, from <code>products</code>. Keep the rows ordered by <code>rn</code>.',
        starter: '-- Add the OVER (ORDER BY price DESC) clause for the row number\nSELECT name, price,\n  ROW_NUMBER() AS rn\nFROM products\nORDER BY rn;',
        solution: 'SELECT name, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM products ORDER BY rn;',
      },
      {
        id: 'm6l2', title: 'RANK within partitions', tag: 'PARTITION BY', xp: 75, ordered: true,
        theory: `<p><code>PARTITION BY</code> restarts the window per group — e.g. rank salaries within each department.</p>
                 <pre>RANK() OVER (
  PARTITION BY department ORDER BY salary DESC)</pre>`,
        task: 'From <code>employees</code>, select <code>department</code>, <code>name</code>, <code>salary</code>, and <code>RANK()</code> as <code>rnk</code> partitioned by <code>department</code> ordered by <code>salary</code> descending. Order the output by <code>department</code>, then <code>rnk</code>.',
        starter: 'SELECT department, name, salary,\n  RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk\nFROM employees\nORDER BY department, rnk;',
        solution: 'SELECT department, name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk FROM employees ORDER BY department, rnk;',
      },
      {
        id: 'm6l3', title: 'Self-join the org chart', tag: 'self-join', xp: 75,
        theory: `<p>A table can join to itself. Here employees link to their manager via <code>manager_id</code>.</p>
                 <pre>FROM employees e
JOIN employees m ON e.manager_id = m.id</pre>`,
        task: 'Return each employee\'s <code>name</code> as <code>employee</code> and their manager\'s <code>name</code> as <code>manager</code>, by self-joining <code>employees</code> on <code>e.manager_id = m.id</code>. (Employees with no manager are excluded by the inner join.)',
        starter: '-- Self-join employees to itself; complete the ON condition\nSELECT e.name AS employee, m.name AS manager\nFROM employees e\nJOIN employees m ON ;',
        solution: 'SELECT e.name AS employee, m.name AS manager FROM employees e JOIN employees m ON e.manager_id = m.id;',
      },
      {
        id: 'm6l4', title: 'Capstone: Top product by revenue', tag: 'capstone', xp: 100, ordered: true,
        theory: `<p>Put it all together: join items to products, compute revenue
                 (<code>price * quantity</code>), aggregate, and rank.</p>
                 <pre>SUM(p.price * oi.quantity) AS revenue</pre>`,
        task: 'Return product <code>name</code> and total <code>revenue</code> (<code>SUM(price * quantity)</code>) across all order items, ordered by <code>revenue</code> descending then <code>name</code> ascending. Join <code>order_items</code> to <code>products</code>.',
        starter: 'SELECT p.name, SUM(p.price * oi.quantity) AS revenue\nFROM order_items oi\nJOIN products p ON p.id = oi.product_id\nGROUP BY p.name\nORDER BY revenue DESC, p.name ASC;',
        solution: 'SELECT p.name, SUM(p.price * oi.quantity) AS revenue FROM order_items oi JOIN products p ON p.id = oi.product_id GROUP BY p.name ORDER BY revenue DESC, p.name ASC;',
      },
    ],
  },
];

// Flat lookup helpers
window.COURSE_INDEX = (function () {
  const lessons = [];
  COURSE.forEach((m) => m.lessons.forEach((l) => lessons.push({ ...l, moduleId: m.id, moduleTitle: m.title })));
  const byId = {};
  lessons.forEach((l, i) => { byId[l.id] = { ...l, order: i }; });
  return { lessons, byId, total: lessons.length };
})();

// Badge definitions (earned by completing a module fully)
window.BADGES = [
  { id: 'm1', icon: '🌱', name: 'Rookie' },
  { id: 'm2', icon: '🔍', name: 'Filter Master' },
  { id: 'm3', icon: '📊', name: 'Aggregator' },
  { id: 'm4', icon: '🔗', name: 'Join Wizard' },
  { id: 'm5', icon: '🧩', name: 'Subquery Sage' },
  { id: 'm6', icon: '🏆', name: 'SQL Champion' },
];
