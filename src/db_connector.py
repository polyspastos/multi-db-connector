import pandas as pd
import logging
import os
import time

from logging.handlers import RotatingFileHandler
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


class DatabaseConnector:
    def __init__(self):
        self._setup_logging()
        load_dotenv(".env")
        self.pg_engine = self._create_postgres_engine()
        self.sqlite_engine = self._create_sqlite_engine()
        self.query_history = []
        self.logger.info("DatabaseConnector initialized")

    def _setup_logging(self):
        if not os.path.exists("logs"):
            os.makedirs("logs")

        self.logger = logging.getLogger("DatabaseConnector")
        self.logger.setLevel(logging.DEBUG)

        if self.logger.handlers:
            self.logger.handlers.clear()

        file_handler = RotatingFileHandler(
            "logs/database.log", maxBytes=5 * 1024 * 1024, backupCount=5
        )
        console_handler = logging.StreamHandler()

        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_format = logging.Formatter("%(levelname)s: %(message)s")
        file_handler.setFormatter(file_format)
        console_handler.setFormatter(console_format)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def _create_postgres_engine(self):
        pg_url = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
        return create_engine(pg_url)

    def _create_sqlite_engine(self):
        return create_engine("sqlite:///test.db")

    def execute_query(self, query, database="postgres"):
        start_time = time.time()
        try:
            engine = self.pg_engine if database == "postgres" else self.sqlite_engine
            with engine.connect() as connection:
                if query.strip().lower().startswith(("select")):
                    # For SELECT queries, use pandas
                    df = pd.read_sql_query(query, connection)
                    rows_returned = len(df)
                else:
                    # For DDL and DML queries, use execute
                    connection.execute(text(query))
                    connection.commit()
                    df = pd.DataFrame()
                    rows_returned = 0

            execution_time = time.time() - start_time

            # Log query execution
            self.query_history.append(
                {
                    "timestamp": datetime.now(),
                    "database": database,
                    "query": query,
                    "execution_time": execution_time,
                    "rows_returned": rows_returned,
                }
            )

            self.logger.info(f"Query executed successfully on {database}")
            self.logger.info(f"Execution time: {execution_time:.2f} seconds")
            self.logger.info(f"Rows returned: {rows_returned}")

            return df
        except Exception as e:
            self.logger.error(f"Error executing query on {database}: {str(e)}")
            raise

    def join_results(self, query1, query2, join_columns, how="inner"):
        """
        Join results from both databases based on specified columns
        """
        df1 = self.execute_query(query1, "postgres")
        df2 = self.execute_query(query2, "sqlite")

        # Handle duplicate columns by adding suffixes
        df_merged = pd.merge(
            df1, df2, on=join_columns, how=how, suffixes=("_pg", "_sqlite")
        )

        return df_merged

    def get_query_stats(self):
        """
        Return analytics about query execution
        """
        df_stats = pd.DataFrame(self.query_history)
        stats = {
            "total_queries": len(df_stats),
            "avg_execution_time": df_stats["execution_time"].mean(),
            "max_execution_time": df_stats["execution_time"].max(),
            "total_rows_returned": df_stats["rows_returned"].sum(),
        }
        return stats

    def cleanup_databases(self):
        """Clean up all tables in both databases"""
        try:
            self.execute_query("DROP TABLE IF EXISTS customers", "sqlite")
            self.execute_query("DROP TABLE IF EXISTS orders", "postgres")
            self.logger.info("Database cleanup completed successfully")
        except Exception as e:
            self.logger.error(f"Database cleanup failed: {str(e)}")
            raise
