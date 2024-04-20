import tkinter as tk
from tkinter import scrolledtext, simpledialog, Label, Entry, Button
import json
import os
from explain import PostgresqlDatabase, CostCalculator

class DatabaseConfigDialog(simpledialog.Dialog):
    def body(self, master):
        Label(master, text="Database Name:").grid(row=0)
        Label(master, text="Username:").grid(row=1)
        Label(master, text="Password:").grid(row=2)
        Label(master, text="Host:").grid(row=3)
        Label(master, text="Port:").grid(row=4)

        self.dbname_entry = Entry(master)
        self.username_entry = Entry(master)
        self.password_entry = Entry(master, show='*')
        self.host_entry = Entry(master)
        self.port_entry = Entry(master)

        self.dbname_entry.grid(row=0, column=1)
        self.username_entry.grid(row=1, column=1)
        self.password_entry.grid(row=2, column=1)
        self.host_entry.grid(row=3, column=1)
        self.port_entry.grid(row=4, column=1)

        return self.dbname_entry
    
    def apply(self):
        self.result = {
            'dbname': self.dbname_entry.get(),
            'username': self.username_entry.get(),
            'password': self.password_entry.get(),
            'host': self.host_entry.get(),
            'port': self.port_entry.get()
        }

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
        self.db_info = {}
        self.db = None
        self.calc = None

    def init_widgets(self):
        # Button to input database details
        self.db_button = tk.Button(self.master, text="Set Database Info", command=self.set_database_info)
        self.db_button.grid(row=0, column=1, padx=10, pady=10, sticky="we")

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

    def set_database_info(self):
        dialog = DatabaseConfigDialog(self.master)
        if dialog.result:
            self.db_info = dialog.result
            self.initialize_database()
    
    def initialize_database(self):
        if self.db_info:
            self.db = PostgresqlDatabase(self.db_info['dbname'], self.db_info['username'],
                                         self.db_info['password'], self.db_info['host'], self.db_info['port'])
            print('Initializing...')
            self.db.get_all_table_details()
            self.calc = CostCalculator(self.db.relation_details, 1024)
            print('Done getting all table details')

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