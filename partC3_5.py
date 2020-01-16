
from mrjob.job import MRJob
import time
import datetime
import re

class PartC3_5(MRJob):

    def mapper(self, _, line):
        if (len(line.split("\t"))==2):
            fields = line.split("\t")
            method = fields[0][1:-1]
            timestamp = int(fields[1][1:-1])
            month = time.strftime("%m",time.gmtime(timestamp)) #returns month
            year = time.strftime("%Y",time.gmtime(timestamp)) #returns year
            joinValue = (year,month)
            yield ((method,joinValue),1)
    def combiner(self, method, count):
        yield(method,sum(count))

    def reducer(self, method, count):
        yield(method,sum(count))

if __name__ == '__main__':
    PartC3_5.run()
