import tkinter as tk
from tkinter import scrolledtext

from project import SQLValidator


def explain_query():  # use sql_query to process the queries
    validator = SQLValidator()
    sql_query = query_entry.get("1.0", tk.END).strip()  # Get the input SQL query
    result = validator.is_valid_sql(sql_query)
    if False in result:
        result_text = f"Error in basic syntax check:\n {result[1]}"
    else:
        result_text = "Explaining costs for the query: \n" + sql_query  # Placeholder text
        # You will want to use the sql query from here only

    results_box.config(state=tk.NORMAL)  # Enable editing
    results_box.delete("1.0", tk.END)  # Clear existing content
    results_box.insert(tk.END, result_text)  # Insert new results
    results_box.insert(tk.END, "Hello")  # Insert new results
    results_box.config(state=tk.DISABLED)  # Disable editing


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
