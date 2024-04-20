import psycopg2
import json
import os
import pickle
import math
import re
import copy

class Relation:
    def __init__(self, name, attributes, distinct_values, num_tuples=0, blocks_in_storage=0):
        self.name = name #Relation name
        self.attributes = attributes #Array of attribute names (not sure if needed since distinct values alr hold all attributes)
        self.distinct_values = distinct_values #Array of Distinct values of each attribute in relation
        self.num_tuples = num_tuples #Number of tuples in relation
        self.blocks_in_storage = blocks_in_storage #Number of blocks to store all tuples

    def update_name(self, name):
        self.name = name

    def update_attributes(self, attributes):
        self.attributes = attributes
    
    def update_distinct_values(self, distinct_values):
        self.distinct_values = distinct_values

    def update_num_tuples(self, num_tuples):
        self.num_tuples = num_tuples

    def update_blocks_in_storage(self, blocks_in_storage):
        self.blocks_in_storage = blocks_in_storage

class PostgresqlDatabase:
    def __init__(self, dbname, username, password, host, port):
        try:
            self.conn = psycopg2.connect("dbname={} user={} password={} host={} port={}".format(dbname, username, password, host, port))
            self.cur = self.conn.cursor()
        except:
            self.conn = None
            self.cur = None
            print("Failed to connect to db")
        self.explain_result = None
        self.query_directory = 'query_results/'
    
    def __del__(self):
        if self.conn is not None:
            self.cur.close()
            self.conn.close()

    def close(self):
        if self.conn is not None:
            self.cur.close()
            self.conn.close()

    def getQEP(self, query):
        self.query = query
        if self.conn is not None:
            self.cur.execute("EXPLAIN (FORMAT JSON) " + query)
            self.explain_result = self.cur.fetchone()[0][0]
            return self.explain_result
        else: 
            filename = self.query_to_json_file_name(self.query)
            self.explain_result = self.JSON_to_QEP(filename)
            return self.explain_result
    
    def display_plan(self):
        if self.explain_result is not None:
            print("Execution Plan:")
            total_cost = self.display_subplan(self.explain_result['Plan'], 0)
            print('\nTotal cost: {:.2f}'.format(total_cost))
        else:
            print("No QEP")

    def display_subplan(self, plan, indent_level):
        cost = plan['Total Cost']
        print("  " * indent_level + plan['Node Type'] + ' - cost: ' + str(plan['Total Cost']))
        if 'Index Name' in plan:
            print("  " * (indent_level + 1) + "Index Name:", plan['Index Name'])
        if 'Relation Name' in plan:
            print("  " * (indent_level + 1) + "Relation Name:", plan['Relation Name'])
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                cost += self.display_subplan(subplan, indent_level + 1)
        return cost

    def get_relation_details(self, relation_name):
        try:
            # Check if the relation exists
            self.cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (relation_name,))
            relation_exists = self.cur.fetchone()[0]

            if not relation_exists:
                print(f"Relation '{relation_name}' does not exist.")
                return

            # Get column names
            self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (relation_name,))
            columns = [row[0] for row in self.cur.fetchall()]

            # Get number of distinct values for each column
            distinct_counts = {}
            for column in columns:
                self.cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {relation_name}")
                distinct_count = self.cur.fetchone()[0]
                distinct_counts[column] = distinct_count

            # Get total number of tuples
            self.cur.execute(f"SELECT COUNT(*) FROM {relation_name}")
            num_tuples = self.cur.fetchone()[0]

            # Get number of blocks/pages in storage
            self.cur.execute(f"SELECT relpages FROM pg_class WHERE relname = %s", (relation_name,))
            blocks_in_storage = self.cur.fetchone()[0]

            relation = Relation(relation_name, columns, distinct_counts, num_tuples, blocks_in_storage)
            return relation
        finally:
            pass

    def get_all_table_details(self):
        try:
            self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
            table_names = [row[0] for row in self.cur.fetchall()]
            self.relation_details = {}
            for name in table_names:
                self.relation_details[name] = self.get_relation_details(name)
            return self.relation_details
        except:
            try:
                return self.pkl_to_relation()
            except:
                print('data.pkl not found')
        finally:
            pass

    def relation_to_pkl(self):
        file_path = os.path.join(self.query_directory, 'data.pkl')
        with open(file_path, 'wb') as file:
            pickle.dump(self.relation_details, file)   

    def pkl_to_relation(self):
        file_path = os.path.join(self.query_directory, 'data.pkl')
        with open(file_path, 'rb') as file:
            self.relation_details = pickle.load(file)
            return self.relation_details

    def query_to_json_file_name(self, query):
        file_name = query.replace(' ', '_').replace('*', '').replace("'", '').replace(';', '') + '.json'
        return file_name

    def QEP_to_JSON(self):
        file_name = self.query_to_json_file_name(self.query)
        file_path = os.path.join(self.query_directory, file_name)
        with open(file_path, 'w') as json_file:
            json.dump(self.explain_result, json_file)

    def JSON_to_QEP(self, json_file):
        file_path = os.path.join(self.query_directory, json_file)
        with open(file_path, 'r') as json_file:
            self.explain_result = json.load(json_file)
            return self.explain_result

    def is_valid_sql(self, query):
        if not self.basic_syntax_check(query):
            return False, "Syntax error"

        success, error = self.database_syntax_check(query)
        return success, error

    def basic_syntax_check(self, query):
        try:
            parsed = sqlparse.parse(query)
            return bool(parsed)
        except Exception as e:
            print(f"Error in basic syntax check: {e}")
            return False

    def database_syntax_check(self, query):
        try:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(query)
                cursor.close()
        except psycopg2.Error as e:
            print(f"SQL execution error: {e}")
            return False, str(e)
        return True, ""

class CostCalculator:
    def __init__(self, relation_details, buffer_size):
        self.relation_details_original = relation_details
        self.relation_details = copy.deepcopy(relation_details)
        self.buffer_size = buffer_size
        self.output = []

    def update_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size

    def calculate_cost(self, qep):
        self.output = []
        self.relation_details = copy.deepcopy(self.relation_details_original)
        self.calculate_cost_subplan(qep['Plan'])

    def calculate_cost_subplan(self, plan):
        cost = 0
        intermediate_relation = []
        if 'Plans' in plan:
            for i,subplan in enumerate(plan['Plans']):
                intermediate_relation.append(self.calculate_cost_subplan(subplan))
        operator = plan['Node Type']
        
        if 'Relation Name' in plan:
            relation = self.relation_details[plan['Relation Name']]
        else:
            relation = intermediate_relation[0][1]
        if len(intermediate_relation) > 1:
            relation_2 = intermediate_relation[1][1]
        
        if len(intermediate_relation) < 2:
            cost = self.get_cost(operator, relation)
            return_relation = self.get_intermediate_relation(plan, relation)
        else: 
            cost = self.get_cost(operator, intermediate_relation[0][1], intermediate_relation[1][1]) 
            return_relation = self.get_intermediate_relation(plan, intermediate_relation[0][1], intermediate_relation[1][1])

        self.output.append((cost,operator))
        return (cost, return_relation)

    def get_intermediate_relation(self, plan, relation_1, relation_2 = None):
        return_relation = relation_1
        operation = plan['Node Type']
        if operation == 'Seq Scan':
            if 'Alias' in plan:
                return_relation.update_name(plan['Alias'])
            if 'Filter' in plan:
                filter_by = plan['Filter']
                conditions = self.process_join_condition(filter_by)
                tuple_count = relation_1.num_tuples
                block_count = relation_1.blocks_in_storage
                for condition in conditions:
                    if condition[1] in ['=','==']:
                        tuple_count = tuple_count/relation_1.distinct_values[condition[0]]
                        block_count = block_count/relation_1.distinct_values[condition[0]]
                        for key,value in return_relation.distinct_values.items():
                            return_relation.distinct_values[key] = math.ceil(value / relation_1.distinct_values[condition[0]])
                    elif condition[1] in ['<>']:
                        pass
                    else:
                        tuple_count = tuple_count/3
                        block_count = block_count/3
                        for key,value in return_relation.distinct_values.items():
                            return_relation.distinct_values[key] = math.ceil(value / 3)
                return_relation.update_num_tuples = tuple_count
                return_relation.update_blocks_in_storage = block_count
        elif operation in ['Sort', 'Hash']:
            return_relation = relation_1
        elif operation in ['Hash Join', 'Sort Merge Join', 'Merge', 'Block Nested Loop Join']:
            for key in plan:
                if 'Cond' in key:
                    conditions = self.process_join_condition(plan[key])
                    break
            attributes = relation_1.attributes + relation_2.attributes
            distinct_values = relation_1.distinct_values
            distinct_values.update(relation_2.distinct_values)
            tuples = relation_1.num_tuples * relation_2.num_tuples
            blocks = relation_1.blocks_in_storage * relation_2.blocks_in_storage
            for condition in conditions:
                tuples = math.ceil(tuples / max(distinct_values[condition[0].split('.')[1]], distinct_values[condition[2].split('.')[1]]))
                blocks = math.ceil(blocks / max(distinct_values[condition[0].split('.')[1]], distinct_values[condition[2].split('.')[1]]))
                attributes.remove(condition[0].split('.')[1])
                if distinct_values[condition[0].split('.')[1]] > distinct_values[condition[2].split('.')[1]]:
                    del distinct_values[condition[0].split('.')[1]]
                else: 
                    del distinct_values[condition[2].split('.')[1]]
                
            return_relation.update_num_tuples(tuples)
            return_relation.update_blocks_in_storage(blocks)
            return_relation.update_distinct_values(distinct_values)
            return_relation.update_attributes(attributes)
        else:
            if relation_2 is None:
                return_relation = relation_1
            else:
                for key in plan:
                    if 'Cond' in key:
                        conditions = self.process_join_condition(plan[key])
                        break
                attributes = relation_1.attributes + relation_2.attributes
                distinct_values = relation_1.distinct_values
                distinct_values.update(relation_2.distinct_values)
                tuples = relation_1.num_tuples * relation_2.num_tuples
                blocks = relation_1.blocks_in_storage * relation_2.blocks_in_storage
                for condition in conditions:
                    tuples = math.ceil(tuples / max(distinct_values[condition[0].split('.')[1]], distinct_values[condition[2].split('.')[1]]))
                    blocks = math.ceil(blocks / max(distinct_values[condition[0].split('.')[1]], distinct_values[condition[2].split('.')[1]]))
                    attributes.pop(condition[0].split('.')[1])
                    if distinct_values[condition[0].split('.')[1]] > distinct_values[condition[2].split('.')[1]]:
                        del distinct_values[condition[0].split('.')[1]]
                    else: 
                        del distinct_values[condition[2].split('.')[1]]

                return_relation.update_num_tuples(tuples)
                return_relation.update_blocks_in_storage(blocks)
                return_relation.update_distinct_values(distinct_values)
                return_relation.update_attributes(attributes)

        return return_relation

    def get_cost(self, operation, relation_1, relation_2 = None):
        cost = 0
        if operation == 'Seq Scan':
            cost = relation_1.blocks_in_storage
        elif operation == 'Sort':
            cost = 2 * relation_1.blocks_in_storage * (math.log(relation_1.blocks_in_storage)/math.log(self.buffer_size))
        elif operation == 'Hash':
            cost = 3 * relation_1.blocks_in_storage
        elif operation in ['Hash Join', 'Sort Merge Join']:
            cost = 3 * (relation_1.blocks_in_storage + relation_2.blocks_in_storage)
        elif operation in ['Merge']:
            cost = relation_1.blocks_in_storage + relation_2.blocks_in_storage
        elif operation == 'Block Nested Loop Join':
            cost = relation_1.blocks_in_storage + (relation_1.blocks_in_storage/(self.buffer_size-1)) * relation_2.blocks_in_storage
        else:
            cost = relation_1.blocks_in_storage

        return cost

    def get_output(self):
        return self.output

    def process_join_condition(self, join_condition):
        # This pattern captures comparison operators including '=', '>', '<', '<>', and their possible combinations
        pattern = r'(\w+\.\w+)\s*([=><!]+)\s*(\w+\.\w+)|(AND|OR)'
        tokens = re.split(r'\s+(AND|OR)\s+', join_condition)  # Split by logical operators

        conditions = []

        # First pass to identify and capture conditions split by logical operators
        for token in tokens:
            if token in ('AND', 'OR'):
                conditions.append(token)
            else:
                # Match comparison operations within the tokens
                match = re.search(r'\((\w+\.\w+|\w+)\s*([=><!]+)\s*(\w+\.\w+|\w+)\)', token)
                if match:
                    conditions.append(match.groups())

        return conditions
