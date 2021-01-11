# Vector_Clock class for ordered reliable multicast
class Vector_Clock:

    def __init__(self, id):
        if id == None:
            self.vector_clock_dictionary = {}
        else:
            self.vector_clock_dictionary = {(id): 0}

    # Increasing clock by one for specific id
    def increase_clock(self, id):
        self.vector_clock_dictionary[(id)] += 1

    # Adds participant to vector clock with timestamp zero (selected by id)
    def add_participant_to_clock(self, id):
        self.vector_clock_dictionary[id] = 0

    # Prints the vector clock
    def print_clock(self):
        print("\n[VECTOR-CLOCK]: ", str(self.vector_clock_dictionary))
