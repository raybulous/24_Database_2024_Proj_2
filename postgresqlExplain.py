import psycopg2

def analyze_query(query):
    conn = psycopg2.connect("dbname=TPC-H user=postgres password=password")
    cur = conn.cursor()
    
    cur.execute("EXPLAIN (FORMAT JSON) " + query)
    explain_result = cur.fetchone()[0][0]
    
    display_query_execution_plan(explain_result)
    
    cur.close()
    conn.close()

def display_query_execution_plan(explain_result):
    total_cost = 0
    plan = explain_result['Plan']
    plan_seq = traverse_query_execution_plan(plan)
    for i, subplan in enumerate(plan_seq):
        total_cost += subplan[1]
        print(subplan[0] + " | cost: " + str(subplan[1]))
        if i != len(plan_seq) - 1:
            print("\t↓")
    
    print('\nTotal cost: {:.2f}'.format(total_cost))    

def traverse_query_execution_plan(plan, plan_seq=[]):
    node = plan.get('Node Type')
    cost = plan.get('Total Cost', 0)

    if 'Plans' in plan:
        for subplan in plan['Plans']:
            plan_seq = traverse_query_execution_plan(subplan, plan_seq)
    
    plan_seq.append((node, cost))
    return plan_seq

# Example usage
query = "SELECT * FROM customer WHERE c_mktsegment = 'BUILDING' ORDER BY c_acctbal DESC LIMIT 10;"
analyze_query(query)
