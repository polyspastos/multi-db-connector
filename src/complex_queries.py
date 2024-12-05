import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from datetime import datetime
from pathlib import Path

from db_connector import DatabaseConnector


pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

db = DatabaseConnector()

print("\n=== Customer Purchase Analysis ===")
customer_analysis = db.join_results(
    query1="""
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_spent,
            AVG(amount) as avg_order_value,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_orders
        FROM orders 
        GROUP BY customer_id
    """,
    query2="""
        SELECT 
            id as customer_id,
            name,
            email,
            created_at as registration_date
        FROM customers
    """,
    join_columns=["customer_id"],
)
print(customer_analysis)

print("\n=== Monthly Revenue Trends ===")
monthly_trends = db.execute_query(
    """
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        COUNT(*) as total_orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(amount) as revenue,
        AVG(amount) as avg_order_value,
        SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY month
""",
    database="postgres",
)
print(monthly_trends)

print("\n=== Customer Status Summary ===")
status_summary = db.execute_query(
    """
    SELECT 
        status,
        COUNT(*) as order_count,
        COUNT(DISTINCT customer_id) as customer_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM orders
    GROUP BY status
""",
    database="postgres",
)
print(status_summary)

stats = db.get_query_stats()
print("\n=== Query Performance Statistics ===")
print(f"Total Queries Run: {stats['total_queries']}")
print(f"Average Execution Time: {stats['avg_execution_time']:.3f} seconds")
print(f"Maximum Execution Time: {stats['max_execution_time']:.3f} seconds")
print(f"Total Rows Returned: {stats['total_rows_returned']}")

print("\n=== Advanced Pandas Analysis ===")

# customer lifetime value
customer_analysis["days_since_registration"] = (
    datetime.now() - pd.to_datetime(customer_analysis["registration_date"])
).dt.days

customer_analysis["daily_value"] = (
    customer_analysis["total_spent"] / customer_analysis["days_since_registration"]
)
print("\nCustomer Daily Value:")
print(
    customer_analysis[
        ["name", "total_spent", "days_since_registration", "daily_value"]
    ].sort_values("daily_value", ascending=False)
)

# order frequency
monthly_trends["month"] = pd.to_datetime(monthly_trends["month"])
monthly_trends["growth_rate"] = monthly_trends["revenue"].pct_change() * 100
print("\nMonthly Growth Rates:")
print(monthly_trends[["month", "revenue", "growth_rate"]])


def get_customer_segment(row):
    if row["total_spent"] >= 300:
        return "High Value"
    elif row["total_spent"] >= 200:
        return "Medium Value"
    return "Low Value"


def describe_customer_value(df):
    value_tiers = {
        "High Value": "Spent $300 or more",
        "Medium Value": "Spent $200-$299",
        "Low Value": "Spent less than $200",
    }

    tier_stats = (
        df.groupby("segment")
        .agg(
            {
                "customer_id": "count",
                "total_spent": ["sum", "mean", "min", "max"],
                "order_count": ["sum", "mean"],
            }
        )
        .round(2)
    )

    print("\nCustomer Segment Details:")
    for tier in value_tiers:
        if tier in tier_stats.index:
            stats = tier_stats.loc[tier]
            print(f"\n{tier} ({value_tiers[tier]}):")
            print(f"- Customer count: {stats[('customer_id', 'count')]}")
            print(f"- Total revenue: ${stats[('total_spent', 'sum')]:,.2f}")
            print(f"- Average spent: ${stats[('total_spent', 'mean')]:,.2f}")
            print(
                f"- Spend range: ${stats[('total_spent', 'min')]:,.2f} - ${stats[('total_spent', 'max')]:,.2f}"
            )
            print(f"- Average orders: {stats[('order_count', 'mean')]:,.1f}")


customer_analysis["segment"] = customer_analysis.apply(get_customer_segment, axis=1)
describe_customer_value(customer_analysis)

numeric_cols = [
    "order_count",
    "total_spent",
    "avg_order_value",
    "completed_orders",
    "days_since_registration",
]
correlation_matrix = customer_analysis[numeric_cols].corr()

plots_dir = Path("plots")
plots_dir.mkdir(exist_ok=True)
print(f"Using directory: {plots_dir}")

plt.figure(figsize=(15, 5))

# plot 1: monthly revenue trend
plt.subplot(131)
plt.plot(monthly_trends["month"], monthly_trends["revenue"], marker="o")
plt.title("Monthly Revenue Trend")
plt.xticks(rotation=45)

# plot 2: customer segments
plt.subplot(132)
segment_counts = customer_analysis["segment"].value_counts()
plt.pie(segment_counts, labels=segment_counts.index, autopct="%1.1f%%")
plt.title("Customer Segments Distribution")

# plot 3: correlation heatmap
plt.subplot(133)
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", center=0, fmt=".2f")
plt.title("Metrics Correlation")

plt.tight_layout()
plt.savefig(plots_dir / "analysis_plots.png")
plt.show()
plt.close()

# correlation analysis
numeric_cols = [
    "order_count",
    "total_spent",
    "avg_order_value",
    "completed_orders",
    "days_since_registration",
]
correlation_matrix = customer_analysis[numeric_cols].corr()

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", center=0)
plt.title("Customer Metrics Correlation")
plt.tight_layout()
plt.savefig(plots_dir / f"correlation_matrix_{timestamp}.png")
plt.close()

plt.figure(figsize=(10, 6))
plt.plot(monthly_trends["month"], monthly_trends["revenue"], marker="o", linewidth=2)
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue ($)")
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(plots_dir / f"revenue_trend_{timestamp}.png")
plt.close()

plt.figure(figsize=(8, 8))
plt.pie(
    segment_counts,
    labels=segment_counts.index,
    autopct="%1.1f%%",
    colors=sns.color_palette("pastel"),
)
plt.title("Customer Segments Distribution")
plt.savefig(plots_dir / f"customer_segments_{timestamp}.png")
plt.close()

print(f"\nPlots have been saved to the '{plots_dir}' directory")

customer_analysis["engagement_score"] = (
    (customer_analysis["order_count"] * 0.3)
    + (customer_analysis["total_spent"] / customer_analysis["total_spent"].max() * 0.4)
    + (customer_analysis["completed_orders"] / customer_analysis["order_count"] * 0.3)
) * 100

coupon_threshold = 90
eligible_customers = customer_analysis[
    customer_analysis["engagement_score"] > coupon_threshold
][["name", "email", "engagement_score"]].sort_values(
    "engagement_score", ascending=False
)

print("\n=== Coupon eligible customers (engagement score > 90) ===")
if len(eligible_customers) > 0:
    print("\nThe following customers qualify for a coupon:")
    for _, customer in eligible_customers.iterrows():
        print(f"- {customer['name']} ({customer['email']})")
        print(f"  engagement score: {customer['engagement_score']:.1f}")
else:
    print("No customers currently qualify for the coupon program (requires score > 90)")
