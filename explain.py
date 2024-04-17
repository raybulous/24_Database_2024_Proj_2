class Relation:
    def __init__(self, name, attributes, distinct_values, num_tuples=0, blocks_in_storage=0):
        self.name = name #Relation name
        self.attributes = attributes #Array of attribute names (not sure if needed since distinct values alr hold all attributes)
        self.distinct_values = distinct_values #Array of Distinct values of each attribute in relation
        self.num_tuples = num_tuples #Number of tuples in relation
        self.blocks_in_storage = blocks_in_storage #Number of blocks to store all tuples 
