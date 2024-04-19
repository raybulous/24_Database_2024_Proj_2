from abc import ABC, abstractmethod
import re

class CostCalculator:
    def __init__(self, relation_details):
        self.relation_details = relation_details

    def calculate_cost(self, qep):
        self.calculate_cost_subplan(qep['Plan'])

    def calculate_cost_subplan(self, plan):
        cost = 0
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                cost += self.calculate_cost_subplan(subplan)
        operator = plan['Node Type']
        cost = 0#based on operator do cost estimator
        return cost

    import re

    def process_join_condition(join_condition):
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
                match = re.search(r'(\w+\.\w+)\s*([=><!]+)\s*(\w+\.\w+)', token)
                if match:
                    conditions.append(match.groups())

        # Process the structured conditions
        i = 0
        while i < len(conditions):
            if isinstance(conditions[i], tuple):
                left_operand, operator, right_operand = conditions[i]
                # Normalize operator symbols for clarity in processing
                if operator == '!=':  # Often used as an alternative to '<>'
                    operator = '<>'
                print(f"Processing condition: {left_operand} {operator} {right_operand}")
            elif conditions[i] in ('AND', 'OR'):
                print(f"Logical operator: {conditions[i]}")
            i += 1

    # Example usage:
    join_condition = "a.id <> b.id AND a.value > b.value OR c.status = d.status"
    process_join_condition(join_condition)


class NodeType(ABC):
    @abstractmethod
    def calculate_cost(self):
        pass

class SeqScan(NodeType):
    def calculate_cost(self, relation_detail):
        blocks = relation_details.blocks_in_storage
        return blocks

class IndexScan(NodeType):
    def calculate_cost(self, relation_detail, index, clustered): 
        distinct_values = relation_detail.distinct_values[index]
        if clustered:
            return relation_detail.blocks_in_storage/distinct_values
        else:
            return relation_detail.num_tuples/distinct_values
