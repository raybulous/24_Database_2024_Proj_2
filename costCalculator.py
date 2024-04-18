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
