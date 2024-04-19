# from postgresql import PostgresqlDatabase
# from databaseServerInfo import DBNAME, USERNAME, PASSWORD, HOST, PORT
#
#
# db = PostgresqlDatabase(DBNAME, USERNAME, PASSWORD, HOST, PORT)
#
# db.get_all_table_details()
# #db.relation_to_pkl()
# #query = "SELECT * FROM customer WHERE c_mktsegment = 'BUILDING' ORDER BY c_acctbal DESC LIMIT 10;"
# #query = "SELECT c_phone FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey WHERE c.c_mktsegment = 'FURNITURE';"
# #query = "SELECT c.c_mktsegment, COUNT(*) as order_count FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY c.c_mktsegment;"
#
# #db.getQEP(query)
# #db.QEP_to_JSON()
# #db.display_plan()
#
# db.close()
import json
from typing import Dict

from explain import Relation
from postgresqlExplain import calculateCost

relation_details = {
    "customer": Relation(500),  # Assume 'customer' table has 500 pages
    "orders": Relation(200)  # Assume 'orders' table has 200 pages
}


def format_qep(plan: Dict, level: int = 0, relation_details: Dict[str, Relation] = {}) -> str:
    indent = "    " * level
    result = f"{indent}Node Type: {plan['Node Type']}\n"

    # Pre-fetch the buffer size
    B = 16384  # TODO: Put in report the calculation

    # Initialize N with 0 which will be updated if there are nested plans
    N = 0
    intermediate_data_handled = False

    # Recursively process sub-plans first to ensure bottom-up calculation
    subplan_results = ""
    if 'Plans' in plan:
        for subplan in plan['Plans']:
            subplan_results += format_qep(subplan, level + 1, relation_details)
            # Aggregate intermediate data volumes where explicit relation names might be missing
            if 'Plan Rows' in subplan:
                N = max(N, subplan['Plan Rows'])  # Use the output rows as a proxy for volume
                intermediate_data_handled = True

    # Append subplans results after processing
    result += subplan_results

    # Fetch M, using Plan Rows if no direct relation is associated
    if 'Relation Name' in plan and plan['Relation Name'] in relation_details:
        M = relation_details[plan['Relation Name']].numPages
    elif 'Plan Rows' in plan:
        M = plan['Plan Rows']  # Use plan rows as a proxy for intermediate data volume
    else:
        M = 0

    # Adjust N if not set by subplans, use M as a fallback
    if not intermediate_data_handled and 'Plan Rows' in plan:
        N = plan['Plan Rows']

    # Calculate cost for the current operation
    cost = calculateCost(plan['Node Type'], Relation(M), Relation(N), B)
    result += f"{indent}Cost: {cost}\n"
    result += f"{indent}Startup Cost: {plan.get('Startup Cost', 'N/A')} - Total Cost: {plan.get('Total Cost', 'N/A')}\n"
    result += f"{indent}Rows: {plan.get('Plan Rows', 'N/A')} Width: {plan.get('Plan Width', 'N/A')}\n"

    # Append relation-specific details
    if 'Relation Name' in plan:
        result += f"{indent}Relation Name: {plan['Relation Name']}\n"
        result += f"{indent}Alias: {plan.get('Alias', 'N/A')}\n"
    if 'Filter' in plan:
        result += f"{indent}Filter: {plan['Filter']}\n"

    return result




def load_json_file(filename: str) -> Dict:
    """Load and return the JSON data from a file."""
    with open(filename, 'r') as file:
        data = json.load(file)
    return data


filename = "query_results/SELECT_c.c_mktsegment,_COUNT()_as_order_count_FROM_orders_o_JOIN_customer_c_ON_o.o_custkey_=_c.c_custkey_GROUP_BY_c.c_mktsegment.json"
qep_data = load_json_file(filename)
formatted_qep_output = format_qep(qep_data['Plan'], 0, relation_details)
print(formatted_qep_output)
