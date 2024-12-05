import unittest
from db_connector import DatabaseConnector


class TestDatabaseConnector(unittest.TestCase):
    def setUp(self):
        self.db = DatabaseConnector()

        try:
            self.db.execute_query(
                """
                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
                "sqlite",
            )

            self.db.execute_query(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    amount DECIMAL,
                    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT
                )
            """,
                "postgres",
            )

            self.db.execute_query(
                """
                INSERT INTO customers (id, name, email, created_at) 
                VALUES 
                    (1, 'John Doe', 'john@example.com', '2024-01-01 10:00:00'),
                    (2, 'Jane Smith', 'jane@example.com', '2024-01-15 11:00:00'),
                    (3, 'Bob Wilson', 'bob@example.com', '2024-02-01 09:00:00'),
                    (4, 'Alice Brown', 'alice@example.com', '2024-02-15 14:00:00')
            """,
                "sqlite",
            )

            self.db.execute_query(
                """
                INSERT INTO orders (id, customer_id, amount, order_date, status)
                VALUES 
                    (1, 1, 100.00, '2024-01-05 14:30:00', 'completed'),
                    (2, 1, 200.00, '2024-02-10 16:45:00', 'completed'),
                    (3, 2, 150.00, '2024-02-15 12:20:00', 'completed'),
                    (4, 3, 300.00, '2024-03-01 09:15:00', 'pending'),
                    (5, 2, 175.00, '2024-03-05 11:30:00', 'completed'),
                    (6, 4, 75.00, '2024-03-10 10:00:00', 'completed')
            """,
                "postgres",
            )

        except Exception as e:
            print(f"Setup failed: {str(e)}")
            raise

    def test_join_queries(self):
        result = self.db.join_results(
            "SELECT * FROM orders",
            "SELECT * FROM customers",
            join_columns=["id"],
            how="inner",
        )
        self.assertTrue(len(result) > 0)


if __name__ == "__main__":
    unittest.main()
