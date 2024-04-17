import psycopg2
from explain import Relation

class PostgresqlDatabase:
    def __init__(self, dbname, username, password, host="localhost", port="5432"): 
        self.conn = psycopg2.connect("dbname={} user={} password={} host={} port={}".format(dbname, username, password, host, port))
        self.cur = self.conn.cursor()
        self.explain_result = None
    
    def __del__(self):
        self.cur.close()
        self.conn.close()

    def close(self):
        self.cur.close()
        self.conn.close()

    def getQEP(self, query):
        self.cur.execute("EXPLAIN (FORMAT JSON) " + query)
        self.explain_result = self.cur.fetchone()[0][0]
        return self.explain_result
    
    def display_plan(self):
        if self.explain_result is not None:
            print("Execution Plan:")
            total_cost = self.display_subplan(self.explain_result['Plan'], 0)
            print('\nTotal cost: {:.2f}'.format(total_cost))
        else:
            print("No QEP")

    def display_subplan(self, plan, indent_level):
        cost = plan['Total Cost']
        print("  " * indent_level + plan['Node Type'] + ' - cost: ' + str(plan['Total Cost']))
        if 'Index Name' in plan:
            print("  " * (indent_level + 1) + "Index Name:", plan['Index Name'])
        if 'Relation Name' in plan:
            print("  " * (indent_level + 1) + "Relation Name:", plan['Relation Name'])
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                cost += self.display_subplan(subplan, indent_level + 1)
        return cost

    def get_relation_details(self, relation_name):
        try:
            # Check if the relation exists
            self.cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (relation_name,))
            relation_exists = self.cur.fetchone()[0]

            if not relation_exists:
                print(f"Relation '{relation_name}' does not exist.")
                return

            # Get column names
            self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (relation_name,))
            columns = [row[0] for row in self.cur.fetchall()]

            # Get number of distinct values for each column
            distinct_counts = {}
            for column in columns:
                self.cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {relation_name}")
                distinct_count = self.cur.fetchone()[0]
                distinct_counts[column] = distinct_count

            # Get total number of tuples
            self.cur.execute(f"SELECT COUNT(*) FROM {relation_name}")
            num_tuples = self.cur.fetchone()[0]

            # Get number of blocks/pages in storage
            self.cur.execute(f"SELECT relpages FROM pg_class WHERE relname = %s", (relation_name,))
            blocks_in_storage = self.cur.fetchone()[0]

            relation = Relation(relation_name, columns, distinct_count, num_tuples, blocks_in_storage)
            return relation
        finally:
            pass

    def get_all_table_details(self):
        try:
            self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
            table_names = [row[0] for row in self.cur.fetchall()]
            relation_details = {}
            for name in table_names:
                relation_details[name] = self.get_relation_details(name)
            return relation_details
        finally:
            pass