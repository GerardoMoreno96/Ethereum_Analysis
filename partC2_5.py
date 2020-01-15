
from mrjob.job import MRJob
import re
import time
import datetime

class partC2_5(MRJob):

    def mapper(self, _, line):
        fields = line.split("\t") #["Scamming", "1527435806"]      947515130000000000
        category_and_time = fields[0].split(",")
        category=category_and_time[0][2:-1]
        timestamp = int(category_and_time[1][2:-2])
        amount = fields[1]
        month = time.strftime("%m",time.gmtime(timestamp)) #returns month
        year = time.strftime("%Y",time.gmtime(timestamp)) #returns year
        year_month = (year,month)
        joinKey = (category,year_month)
        yield(joinKey,int(amount))

    def combiner(self,address,values):
        yield(address,sum(values))
    def reducer(self, address, values):
        yield(address,sum(values)) #this gives the output of scamsThorughtime.txt

if __name__ == '__main__':
    partC2_5.run()
