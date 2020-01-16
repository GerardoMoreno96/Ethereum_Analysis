
from mrjob.job import MRJob
import re
import time
import datetime

class partC2(MRJob):

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
                value = fields[3]
                timestamp = fields[6]
                yield (address,(timestamp,2,int(value)))

    def reducer(self, address, values):
        print_flag = False
        date=[]
        amount=[]
        method=""
        count=0
        for i in values:
            if i[1]==1:
                print_flag=True
                method = i[0]
            elif i[1]==2:
                date.append(i[0])
                amount.append(i[2])
        if (print_flag):
            for i in range(len(date)):
                joinKey = (method,date[i])
                yield(joinKey,amount[i])
        print_flag=False
        date=0
        amount=0

if __name__ == '__main__':
    partC2.JOBCONF= { 'mapreduce.job.reduces': '10' }
    partC2.run()
