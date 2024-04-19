import psycopg2
import sqlparse
from databaseServerInfo import DBNAME, USERNAME, PASSWORD, HOST

class SQLValidator:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                dbname=DBNAME,
                user=USERNAME,
                password=PASSWORD,
                host=HOST
            )
        except psycopg2.Error as e:
            print(f"Failed to connect to the database: {e}")
            self.connection = None  # Ensure connection is None if it fails

    def __del__(self):
        if self.connection:
            self.connection.close()

    def is_valid_sql(self, query):
        if not self.basic_syntax_check(query):
            return False, "Syntax error"

        success, error = self.database_syntax_check(query)
        return success, error

    def basic_syntax_check(self, query):
        try:
            parsed = sqlparse.parse(query)
            return bool(parsed)
        except Exception as e:
            print(f"Error in basic syntax check: {e}")
            return False

    def database_syntax_check(self, query):
        try:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(query)
                cursor.close()
        except psycopg2.Error as e:
            print(f"SQL execution error: {e}")
            return False, str(e)
        return True, ""


# Example usage:
validator = SQLValidator()
if validator.connection is not None:
    print(validator.is_valid_sql("SELECT * FROM table;"))  # Likely False, 'table' does not exist
    print(validator.is_valid_sql("SELECT * FROM information_schema.tables;"))  # True
    print(validator.is_valid_sql("SELECT FROM WHERE;"))  # False, incorrect SQL
else:
    print("Database connection could not be established.")
