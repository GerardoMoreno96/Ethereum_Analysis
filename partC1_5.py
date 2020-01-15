
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class partC1_5(MRJob):

    def mapper(self, _, line):
        if(len(line.split("\t"))==2):
            fields = line.split("\t")
            method = fields[0][1:-1]
            value = fields[1]
            yield (method,int(value))

    def combiner(self,method,values):
        yield(method,sum(values))
    def reducer(self, method, values):
        yield(method,sum(values))

if __name__ == '__main__':
    partC1_5.run()
