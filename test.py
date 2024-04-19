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
    B = 1000000  # Example buffer size, adjust based on your actual DB setup

    # Initialize N with 0 which will be updated if there are nested plans
    N = 0

    # Recursively process sub-plans first to ensure bottom-up calculation
    subplan_results = ""
    if 'Plans' in plan:
        for subplan in plan['Plans']:
            subplan_results += format_qep(subplan, level + 1, relation_details)
            sub_relation_name = subplan.get('Relation Name', None)
            if sub_relation_name and sub_relation_name in relation_details:
                sub_N = relation_details[sub_relation_name].numPages
                N = max(N, sub_N)  # Get the maximum N from subplans

    # Append subplans results after processing
    result += subplan_results

    # Fetch M, the number of pages for the current relation if available
    M = relation_details.get(plan.get('Relation Name'), Relation(0)).numPages

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


filename = "query_results/SELECT__FROM_customer_WHERE_c_mktsegment_=_BUILDING_ORDER_BY_c_acctbal_DESC_LIMIT_10.json"
qep_data = load_json_file(filename)
formatted_qep_output = format_qep(qep_data['Plan'], 0, relation_details)
print(formatted_qep_output)
