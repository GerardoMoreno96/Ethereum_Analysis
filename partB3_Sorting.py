from mrjob.job import MRJob
import re

class partB3_Sorting(MRJob):

    def mapper(self, _, line):
        ds_row = line.split("\t") #split by tab
        contract = ds_row[0] #get contract address
        amount = ds_row[1] #get value
        yield(None,(contract,int(amount))) #everything should go to the same reducer

    def combiner(self,_, values):
        sorted_values = sorted(values, reverse=True, key=lambda values: values[1])
        i = 0
        for value in sorted_values:
            yield("top",value)
            i+=1
            if i>=10:
                break

    def reducer(self,_, values):
        sorted_values = sorted(values, reverse=True, key=lambda values: values[1])
        i = 0
        for value in sorted_values:
            yield(value[0],value[1])

            i+=1
            if i>=10:
                break

if __name__ == '__main__':
    partB3_Sorting.run()
