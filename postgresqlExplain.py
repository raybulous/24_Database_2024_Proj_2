import psycopg2

def analyze_query(query):
    conn = psycopg2.connect("dbname=TPC-H user=postgres password=comsolag")
    cur = conn.cursor()
    
    cur.execute("EXPLAIN (FORMAT JSON) " + query)
    explain_result = cur.fetchone()[0][0]
    display_plan(explain_result)

    cur.close()
    conn.close()

def display_plan(explain_result):
    print("Execution Plan:")
    total_cost = display_subplan(explain_result['Plan'], 0)
    print('\nTotal cost: {:.2f}'.format(total_cost))

def display_subplan(plan, indent_level):
    cost = plan['Total Cost']
    print("  " * indent_level + plan['Node Type'] + ' - cost: ' + str(plan['Total Cost']))
    if 'Index Name' in plan:
        print("  " * (indent_level + 1) + "Index Name:", plan['Index Name'])
    if 'Relation Name' in plan:
        print("  " * (indent_level + 1) + "Relation Name:", plan['Relation Name'])
    if 'Plans' in plan:
        for subplan in plan['Plans']:
            cost += display_subplan(subplan, indent_level + 1)
    return cost

#query = "SELECT * FROM customer WHERE c_mktsegment = 'BUILDING' ORDER BY c_acctbal DESC LIMIT 10;"
#analyze_query(query)

#query = "SELECT c_phone FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey WHERE c.c_mktsegment = 'FURNITURE';"
#analyze_query(query)

query = "SELECT c.c_mktsegment, COUNT(*) as order_count FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY c.c_mktsegment;"
analyze_query(query)