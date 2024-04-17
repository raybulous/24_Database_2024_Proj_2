import psycopg2

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