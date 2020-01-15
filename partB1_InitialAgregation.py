
from mrjob.job import MRJob
import re

#aggregate transactions
#http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1574975221160_2423/
class partB1_InitialAgregation(MRJob):

   def mapper(self, _, line):
        try:
            fields = line.split(",")
            toAddress = fields[2]
            value = fields[3]
            yield(toAddress,int(value))
        except:
            pass

   def combiner(self,address,count):
        yield (address, sum(count))

   def reducer(self,address,count):
        yield (address, sum(count)) #this outputs toptenOutput1.tsv


if __name__ == '__main__':
    partB1_InitialAgregation.run()
