from postgresql import PostgresqlDatabase

db = PostgresqlDatabase("TPC-H", "postgres", "comsolag")

db.get_all_table_details()
#db.relation_to_pkl()
#query = "SELECT * FROM customer WHERE c_mktsegment = 'BUILDING' ORDER BY c_acctbal DESC LIMIT 10;"
#query = "SELECT c_phone FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey WHERE c.c_mktsegment = 'FURNITURE';"
#query = "SELECT c.c_mktsegment, COUNT(*) as order_count FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY c.c_mktsegment;"

#db.getQEP(query)
#db.QEP_to_JSON()
#db.display_plan()

db.close()