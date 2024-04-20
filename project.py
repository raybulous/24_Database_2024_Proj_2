# Create the main window and pass it to the QEPInterface
import tkinter as tk
from tkinter import scrolledtext
import json
import os

from explain import CostCalculator, PostgresqlDatabase
from interface import QEPInterface

def handle_query(sql_query):
    # Clear previous results and QEP information
    app.results_box.config(state=tk.NORMAL)
    app.results_box.delete("1.0", tk.END)
    app.qep_box.config(state=tk.NORMAL)
    app.qep_box.delete("1.0", tk.END)

    # Insert the new query processing message
    app.results_box.insert(tk.END, f"Processing query: {sql_query}\n")
    app.results_box.config(state=tk.DISABLED)

    # Check SQL syntax before execution
    valid_sql= app.db.is_valid_sql(sql_query)
    if not valid_sql:
        app.results_box.config(state=tk.NORMAL)
        app.results_box.insert(tk.END, f"Syntax error in SQL query\n")
        app.results_box.config(state=tk.DISABLED)
        return

    # If the syntax is valid, proceed with getting the QEP and calculating costs
    qep = app.db.getQEP(sql_query)
    if qep:
        app.calc.calculate_cost(qep)
        array_output = app.calc.get_output()

        app.display_qep(qep)
        app.display_array(array_output)
    else:
        app.results_box.config(state=tk.NORMAL)
        app.results_box.insert(tk.END, "Failed to retrieve QEP for the query. Please Check Syntax error\n")
        app.results_box.config(state=tk.DISABLED)


# db = PostgresqlDatabase(DBNAME, USERNAME, PASSWORD, HOST, PORT)
# db.get_all_table_details()
# calc = CostCalculator(db.relation_details, 1024)
# print('Done getting all table details')

# Interface stuff
root = tk.Tk()
app = QEPInterface(root, on_query_submit=handle_query)
root.mainloop()