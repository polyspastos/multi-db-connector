from db_connector import DatabaseConnector

db = DatabaseConnector()

print("\nCustomers (SQLite):")
customers = db.execute_query("SELECT * FROM customers", database="sqlite")
print(customers)

print("\nOrders (PostgreSQL):")
orders = db.execute_query("SELECT * FROM orders", database="postgres")
print(orders)

print("\nCustomer order summary:")
customer_orders = db.execute_query(
    """
    SELECT customer_id, 
           COUNT(*) as order_count, 
           SUM(amount) as total_spent,
           AVG(amount) as avg_order_value
    FROM orders 
    GROUP BY customer_id
    ORDER BY total_spent DESC
""",
    database="postgres",
)
print(customer_orders)

print("\nCustomers with their orders:")
joined_data = db.join_results(
    query1="SELECT id as customer_id, amount FROM orders",
    query2="SELECT id as customer_id, name, email FROM customers",
    join_columns=["customer_id"],
)
print(joined_data)

stats = db.get_query_stats()
print("\nDetailed query statistics:")
print(f"Total Queries Run: {stats['total_queries']}")
print(f"Average Execution Time: {stats['avg_execution_time']:.3f} seconds")
print(f"Maximum Execution Time: {stats['max_execution_time']:.3f} seconds")
print(f"Total Rows Returned: {stats['total_rows_returned']}")
