# Create the main window and pass it to the QEPInterface
import tkinter as tk
from tkinter import scrolledtext
import json
import os

from explain import CostCalculator, PostgresqlDatabase
from interface import QEPInterface
from databaseServerInfo import DBNAME, USERNAME, PASSWORD, HOST, PORT

def handle_query(sql_query):
    app.results_box.config(state=tk.NORMAL)
    app.results_box.insert(tk.END, f"Processing query: {sql_query}\n")
    app.results_box.config(state=tk.DISABLED)

    qep = app.db.getQEP(sql_query)
    app.calc.calculate_cost(qep)

    app.display_qep(qep)
    app.display_array(app.calc.get_output())


# db = PostgresqlDatabase(DBNAME, USERNAME, PASSWORD, HOST, PORT)
# db.get_all_table_details()
# calc = CostCalculator(db.relation_details, 1024)
# print('Done getting all table details')

# Interface stuff
root = tk.Tk()
app = QEPInterface(root, on_query_submit=handle_query)
root.mainloop()