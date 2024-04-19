import math
import re

class CostCalculator:
    def __init__(self, relation_details, buffer_size):
        self.relation_details = relation_details
        self.buffer_size = buffer_size
        self.output = []

    def update_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size

    def calculate_cost(self, qep):
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
