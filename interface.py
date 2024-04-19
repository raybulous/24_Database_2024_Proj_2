import json
import tkinter as tk
from tkinter import scrolledtext
from typing import Dict

from explain import Relation
from postgrepayload import PostgresqlDatabase1
from postgresqlExplain import calculateCost
from databaseServerInfo import DBNAME, USERNAME, PASSWORD, HOST, PORT

sql_server = PostgresqlDatabase1(DBNAME, USERNAME, PASSWORD, HOST, PORT)


def explain_query():  # use sql_query to process the queries
    validator = sql_server
    sql_query = query_entry.get("1.0", tk.END).strip()  # Get the input SQL query
    result = validator.is_valid_sql(sql_query)
    if False in result:
        result_text = f"Error in basic syntax check:\n {result[1]}"
    else:
        result_text = "Explaining costs for the query: \n" + sql_query  # Placeholder text
        qep = validator.getQEP(sql_query)
        formatted_qep = format_qep(qep['Plan'], 0,
                                   relation_details)  # Assuming the QEP is wrapped in a dictionary under 'Plan'
        result_text += "\n" + formatted_qep

    results_box.config(state=tk.NORMAL)  # Enable editing
    results_box.delete("1.0", tk.END)  # Clear existing content
    results_box.insert(tk.END, result_text)  # Insert new results
    results_box.insert(tk.END, "Hello")  # Insert new results
    results_box.config(state=tk.DISABLED)  # Disable editing


# TODO: Replace relation_details with actual dictionary that consist of all relations
relation_details = {
    "customer": Relation(500),  # Assume 'customer' table has 500 pages
    "orders": Relation(200)  # Assume 'orders' table has 200 pages
}


def format_qep(plan: Dict, level: int = 0, relation_details: Dict[str, Relation] = {}):
    indent = "    " * level
    result = f"{indent}Node Type: {plan['Node Type']}\n"

    # Retrieve or estimate the number of pages (numPages) for the relations
    # You might need to adjust this based on actual schema and data distribution
    # TODO: Retrieve the number of pages for all relations involved
    M = relation_details.get(plan.get('Relation Name'), Relation(0)).numPages
    B = 4  # Example buffer size, you may need to adjust this based on your actual DB setup or calculations

    if 'Plans' in plan and plan['Plans']:
        N = relation_details.get(plan['Plans'][0].get('Relation Name'), Relation(0)).numPages
    else:
        N = 0

    # Calculate cost for the current operation
    cost = calculateCost(plan['Node Type'], Relation(M), Relation(N), B)
    result += f"{indent}Cost: {cost}\n"

    # Include more details in the result if necessary
    result += f"{indent}Startup Cost: {plan.get('Startup Cost', 'N/A')} - Total Cost: {plan.get('Total Cost', 'N/A')}\n"
    result += f"{indent}Rows: {plan.get('Plan Rows', 'N/A')} Width: {plan.get('Plan Width', 'N/A')}\n"

    if 'Relation Name' in plan:
        result += f"{indent}Relation Name: {plan['Relation Name']}\n"
        result += f"{indent}Alias: {plan.get('Alias', 'N/A')}\n"
    if 'Filter' in plan:
        result += f"{indent}Filter: {plan['Filter']}\n"

    for subplan in plan.get('Plans', []):
        result += format_qep(subplan, level + 1, relation_details)

    return result


# Create the main window
window = tk.Tk()
window.title("QEP Cost Estimation Interface")

# Configure grid layout to expand with the window resizing
window.columnconfigure(0, weight=1)  # Column with text widget will expand
window.rowconfigure(1, weight=1)  # Row with text widget will expand

# Create a label for the SQL query input
label_input = tk.Label(window, text="Enter SQL Query:")
label_input.grid(row=0, column=0, padx=10, pady=10, sticky="we")

# Create a text entry widget for SQL queries
query_entry = tk.Text(window, height=10)  # You can adjust height as needed
query_entry.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")

# Button to trigger the cost explanation
explain_button = tk.Button(window, text="Explain Costs", command=explain_query)
explain_button.grid(row=2, column=0, padx=10, pady=10, sticky="we")

# Scrolled text widget for displaying results
results_box = scrolledtext.ScrolledText(window, state='disabled', height=10, width=70)
results_box.grid(row=3, column=0, padx=10, pady=10, sticky="we")

# Start the GUI event loop
window.mainloop()
