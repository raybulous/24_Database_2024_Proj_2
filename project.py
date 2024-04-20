# Create the main window and pass it to the QEPInterface
import tkinter as tk
from tkinter import scrolledtext
import json
import os

from costCalculator import CostCalculator
from interface import QEPInterface
from postgresql import PostgresqlDatabase
from databaseServerInfo import DBNAME, USERNAME, PASSWORD, HOST, PORT

def handle_query(sql_query):
    app.results_box.config(state=tk.NORMAL)
    app.results_box.insert(tk.END, f"Processing query: {sql_query}")
    app.results_box.config(state=tk.DISABLED)

    # TODO: Include logic to process the query and potentially fetch and display results
    qep = db.getQEP(sql_query)
    calc.calculate_cost(qep)
    array_output = calc.get_output()

    # TODO: replace json_path with new json generated from query
    # json_path = "query_results/SELECT_c.c_mktsegment,_COUNT()_as_order_count_FROM_orders_o_JOIN_customer_c_ON_o.o_custkey_=_c.c_custkey_GROUP_BY_c.c_mktsegment.json"
    # with open(json_path, 'r') as file:
    #     json_data = json.load(file)
    #     app.display_qep(json_data)

    app.display_qep(qep)

    # array_1 = ["Seq Scan", 10, "Hash", 0, "Seq Scan", 10, "Hash Join", 20, "Aggregate", 10, "Sort", 30, "Gather Merge", 10, "Aggregate", 10]
    # app.display_array(array_1)

    app.display_array(calc.get_output())



db = PostgresqlDatabase(DBNAME, USERNAME, PASSWORD, HOST, PORT)
db.get_all_table_details()
calc = CostCalculator(db.relation_details, 1024)
print('Done getting all table details')

# Interface stuff
root = tk.Tk()
app = QEPInterface(root, on_query_submit=handle_query)
root.mainloop()


# TODO: You can paste this without the quotes into the interface, it will work
# query = "SELECT * FROM customer WHERE c_mktsegment = 'BUILDING' ORDER BY c_acctbal DESC LIMIT 10;"