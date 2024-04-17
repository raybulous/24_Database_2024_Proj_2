import sqlite3
import sqlparse


class SQLValidator:
    def __init__(self):
        self.connection = sqlite3.connect(':memory:')  # Use an in-memory database

    def __del__(self):
        self.connection.close()  # Ensure the database connection is closed

    def is_valid_sql(self, query):
        # First, use sqlparse to check basic syntactical correctness
        if not self.basic_syntax_check(query):
            return False

        # Then, use SQLite engine to check if the SQL can be executed
        return self.database_syntax_check(query), ""

    def basic_syntax_check(self, query):
        try:
            # Parse the query and check if it results in a valid structure
            parsed = sqlparse.parse(query)
            return bool(parsed)  # sqlparse does not throw an error; it returns non-empty on valid SQL
        except Exception as e:
            print(f"Error in basic syntax check: {e}")
            return False

    def database_syntax_check(self, query):
        try:
            # Attempt to execute the query in a transaction that will be rolled back
            with self.connection:
                self.connection.execute(query)
        except sqlite3.Error as e:
            print(f"SQL execution error: {e}")
            return False, e
        return True


# Example usage:
validator = SQLValidator()
print(validator.is_valid_sql("SELECT * FROM table;"))  # False, as 'table' does not exist
# print(validator.is_valid_sql("SELECT * FROM sqlite_master;"))  # True, sqlite_master should exist
# print(validator.is_valid_sql("SELECT FROM WHERE;"))  # False, incorrect SQL
