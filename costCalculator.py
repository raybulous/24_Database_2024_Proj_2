from abc import ABC, abstractmethod

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
