"""partA - Time Analysis"""
from mrjob.job import MRJob
import re
import time
import datetime

#http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1574975221160_2418/
class partA_TimeAnalysis(MRJob):

   def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) ==7:
                time_epoch = int(fields[6])
                month = time.strftime("%m",time.gmtime(time_epoch)) #returns month
                year = time.strftime("%Y",time.gmtime(time_epoch)) #returns year
                year_month = (year,month)
                yield(year_month,1)
        except:
            pass

   def combiner(self,month,count):
        yield (month, sum(count))

   def reducer(self,month,count):
        yield (month, sum(count))


if __name__ == '__main__':
    partA_TimeAnalysis.run()
