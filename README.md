# Your Project Name

This project is a cost estimation of a given QEP provided by postgresql when a query is provided.

## Installation

1. Install the required dependencies by running:
    ```
    pip install -r requirements.txt
    ```

## Configuration

1. Open `databaseServerInfo.py` and change the details to match your database information.

## Running the Program

- If you have the database:
  - After configuring the database details, you can run the program and input your SQL queries. The program will estimate the cost and display the query execution plan.
- If you do not have the database:
  - You can still run the program using a set of prepared queries provided. Simply run the program and input the prepared queries.
  - SELECT * FROM customer WHERE c_mktsegment = 'BUILDING' ORDER BY c_acctbal DESC LIMIT 10;
  - SELECT c_phone FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey WHERE c.c_mktsegment = 'FURNITURE';
  - SELECT c.c_mktsegment, COUNT(*) as order_count FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY c.c_mktsegment;
  - SELECT * FROM customer c WHERE c.c_mktsegment = 'FURNITURE' OR c.c_mktsegment = 'BUILDING';
  - SELECT * FROM customer c WHERE c.c_mktsegment != 'FURNITURE';

## Note

- The program might take a while to start up as it queries the database for all the necessary relation details required for cost calculation.

## Usage

1. Run the program.
2. Enter your SQL query in the prompt.
3. The estimated cost of the query will be shown on the left, and the query execution plan will be shown on the right.


