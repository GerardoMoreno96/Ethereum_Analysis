
from mrjob.job import MRJob
import re

class PartC3(MRJob):

    def mapper(self, _, line):
        if (len(line.split(","))==3):
            fields = line.split(",")
            address = fields[0]
            scamMethod = fields[1]
            yield (address,(scamMethod,1))


        elif(len(line.split(","))==7):
            if not(line.startswith("block_number")):
                fields = line.split(",")
                address = fields[2]
                timestamp = fields[6]
                yield (address,(timestamp,2))

    def reducer(self, address, values):
        print_flag = False
        val=[]
        method=""
        count=0
        for i in values:
            if i[1]==1:
                print_flag=True
                method = i[0]
            elif i[1]==2:
                val.append(i[0])

        if (print_flag):
            for i in val:
                yield(method,i) #this gives the output of lucrativetime
        print_flag=False
        val=0

if __name__ == '__main__':
    PartC3.JOBCONF= { 'mapreduce.job.reduces': '15' }
    PartC3.run()
