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
