# Create the main window and pass it to the QEPInterface
import tkinter as tk
from tkinter import scrolledtext
import json
import os

from interface import QEPInterface





def handle_query(sql_query):
    app.results_box.config(state=tk.NORMAL)
    app.results_box.insert(tk.END, f"Processing query: {sql_query}")
    app.results_box.config(state=tk.DISABLED)
    # Include logic to process the query and potentially fetch and display results

    #TODO: replace json_path with new json generated from query
    json_path = "query_results/SELECT_c.c_mktsegment,_COUNT()_as_order_count_FROM_orders_o_JOIN_customer_c_ON_o.o_custkey_=_c.c_custkey_GROUP_BY_c.c_mktsegment.json"
    with open(json_path, 'r') as file:
        json_data = json.load(file)
        app.display_qep(json_data)

    array_1 = ["Seq Scan", 10, "Hash", 0, "Seq Scan", 10, "Hash Join", 20, "Aggregate", 10, "Sort", 30, "Gather Merge", 10, "Aggregate", 10]
    app.display_array(array_1)



root = tk.Tk()
app = QEPInterface(root, on_query_submit=handle_query)
root.mainloop()