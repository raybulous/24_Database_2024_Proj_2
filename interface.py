# import json
# import tkinter as tk
# from tkinter import scrolledtext
# from typing import Dict
#
# from explain import Relation
# from postgrepayload import PostgresqlDatabase1
# from postgresqlExplain import calculateCost
# from databaseServerInfo import DBNAME, USERNAME, PASSWORD, HOST, PORT
#
# sql_server = PostgresqlDatabase1(DBNAME, USERNAME, PASSWORD, HOST, PORT)
#
#
# def explain_query():  # use sql_query to process the queries
#     validator = sql_server
#     sql_query = query_entry.get("1.0", tk.END).strip()  # Get the input SQL query
#     result = validator.is_valid_sql(sql_query)
#     if False in result:
#         result_text = f"Error in basic syntax check:\n {result[1]}"
#     else:
#         result_text = "Explaining costs for the query: \n" + sql_query  # Placeholder text
#         qep = validator.getQEP(sql_query)
#         formatted_qep = format_qep(qep['Plan'], 0,
#                                    relation_details)  # Assuming the QEP is wrapped in a dictionary under 'Plan'
#         result_text += "\n" + formatted_qep
#
#     results_box.config(state=tk.NORMAL)  # Enable editing
#     results_box.delete("1.0", tk.END)  # Clear existing content
#     results_box.insert(tk.END, result_text)  # Insert new results
#     results_box.insert(tk.END, "Hello")  # Insert new results
#     results_box.config(state=tk.DISABLED)  # Disable editing
#
#
# # TODO: Replace relation_details with actual dictionary that consist of all relations
# relation_details = {
#     "customer": Relation(500),  # Assume 'customer' table has 500 pages
#     "orders": Relation(200)  # Assume 'orders' table has 200 pages
# }
#
#
# def format_qep(plan: Dict, level: int = 0, relation_details: Dict[str, Relation] = {}):
#     indent = "    " * level
#     result = f"{indent}Node Type: {plan['Node Type']}\n"
#
#     # Retrieve or estimate the number of pages (numPages) for the relations
#     # You might need to adjust this based on actual schema and data distribution
#     # TODO: Retrieve the number of pages for all relations involved
#     M = relation_details.get(plan.get('Relation Name'), Relation(0)).numPages
#     B = 4  # Example buffer size, you may need to adjust this based on your actual DB setup or calculations
#
#     if 'Plans' in plan and plan['Plans']:
#         N = relation_details.get(plan['Plans'][0].get('Relation Name'), Relation(0)).numPages
#     else:
#         N = 0
#
#     # Calculate cost for the current operation
#     cost = calculateCost(plan['Node Type'], Relation(M), Relation(N), B)
#     result += f"{indent}Cost: {cost}\n"
#
#     # Include more details in the result if necessary
#     result += f"{indent}Startup Cost: {plan.get('Startup Cost', 'N/A')} - Total Cost: {plan.get('Total Cost', 'N/A')}\n"
#     result += f"{indent}Rows: {plan.get('Plan Rows', 'N/A')} Width: {plan.get('Plan Width', 'N/A')}\n"
#
#     if 'Relation Name' in plan:
#         result += f"{indent}Relation Name: {plan['Relation Name']}\n"
#         result += f"{indent}Alias: {plan.get('Alias', 'N/A')}\n"
#     if 'Filter' in plan:
#         result += f"{indent}Filter: {plan['Filter']}\n"
#
#     for subplan in plan.get('Plans', []):
#         result += format_qep(subplan, level + 1, relation_details)
#
#     return result
#
#
# # Create the main window
# window = tk.Tk()
# window.title("QEP Cost Estimation Interface")
#
# # Configure grid layout to expand with the window resizing
# window.columnconfigure(0, weight=1)  # Column with text widget will expand
# window.rowconfigure(1, weight=1)  # Row with text widget will expand
#
# # Create a label for the SQL query input
# label_input = tk.Label(window, text="Enter SQL Query:")
# label_input.grid(row=0, column=0, padx=10, pady=10, sticky="we")
#
# # Create a text entry widget for SQL queries
# query_entry = tk.Text(window, height=10)  # You can adjust height as needed
# query_entry.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
#
# # Button to trigger the cost explanation
# explain_button = tk.Button(window, text="Explain Costs", command=explain_query)
# explain_button.grid(row=2, column=0, padx=10, pady=10, sticky="we")
#
# # Scrolled text widget for displaying results
# results_box = scrolledtext.ScrolledText(window, state='disabled', height=10, width=70)
# results_box.grid(row=3, column=0, padx=10, pady=10, sticky="we")
#
# # Start the GUI event loop
# window.mainloop()


## new code

import tkinter as tk
from tkinter import scrolledtext
import json
import os


class QEPInterface:
    def __init__(self, master, on_query_submit=None):
        self.master = master
        self.master.title("QEP Cost Estimation Interface")
        self.on_query_submit = on_query_submit  # Callback for when a query is submitted

        # Configure grid layout
        self.master.columnconfigure([0, 1], weight=1)
        self.master.rowconfigure(1, weight=1)
        self.master.rowconfigure(3, weight=2)

        # Initialize widgets
        self.init_widgets()

    def init_widgets(self):
        # Create a label for the SQL query input
        label_input = tk.Label(self.master, text="Enter SQL Query:")
        label_input.grid(row=0, column=0, padx=10, pady=10, sticky="we")

        # Text entry widget for SQL queries
        self.query_entry = tk.Text(self.master, height=10)
        self.query_entry.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")

        # Button to trigger the processing and display of QEP
        process_button = tk.Button(self.master, text="Process Query", command=self.process_query)
        process_button.grid(row=2, column=0, padx=10, pady=10, sticky="we")

        # Scrolled text widget for displaying results
        self.results_box = scrolledtext.ScrolledText(self.master, state='disabled', height=10, width=70)
        self.results_box.grid(row=3, column=0, padx=10, pady=10, sticky="we")

        # Scrolled text widget for displaying the QEP JSON
        self.qep_box = scrolledtext.ScrolledText(self.master, state='disabled', height=10, width=70)
        self.qep_box.grid(row=3, column=1, padx=10, pady=10, sticky="we")

    def process_query(self):
        # Get the input SQL query
        sql_query = self.query_entry.get("1.0", tk.END).strip()
        if self.on_query_submit:
            self.on_query_submit(sql_query)  # This should trigger the external processing and the print statement

    def display_qep(self, qep_json):
        # Format and display the QEP data in a cleaned-up format
        if isinstance(qep_json, str):
            # If a path is passed, read the JSON file
            if os.path.exists(qep_json):
                with open(qep_json, 'r') as file:
                    qep_json = json.load(file)
            else:
                print("File does not exist.")
                return

        qep_text = self.format_qep_data(qep_json)
        self.qep_box.config(state=tk.NORMAL)
        self.qep_box.delete("1.0", tk.END)
        self.qep_box.insert(tk.END, qep_text)
        self.qep_box.config(state=tk.DISABLED)

    def display_array(self, operations):
        # Initialize the display content
        display_content = ""

        # Format the array into a string with each element on a new line from bottom to top
        for index in range(len(operations)-1, -1, -1):
            operation = operations[index][0]
            cost = operations[index][1]
            display_content = f"{operation} {cost}\n" + display_content  # Prepend to display from bottom to top

        # Display in the qep_box
        self.results_box.config(state=tk.NORMAL)
        self.results_box.delete("1.0", tk.END)
        self.results_box.insert(tk.END, display_content)
        self.results_box.config(state=tk.DISABLED)

    def format_qep_data(self, data, depth=0):
        if not isinstance(data, dict):
            return ""

        result = ""
        indent = "    " * depth
        # Check for each key directly in the node and format accordingly
        if "Node Type" in data:
            result += f"{indent}-Node Type: {data['Node Type']}\n"
        if "Total Cost" in data:
            result += f"{indent}Total Cost: {data['Total Cost']:.2f}\n"
        if "Plans" in data and isinstance(data["Plans"], list):  # Ensure 'Plans' is a list before iterating
            result += f"{indent}Plans:\n"
            for plan in data["Plans"]:
                result += self.format_qep_data(plan, depth + 1)
        elif "Plan" in data:  # Sometimes the root element might be "Plan" instead of "Plans"
            result += self.format_qep_data(data["Plan"], depth)  # Process the single plan
        result += "\n"
        return result