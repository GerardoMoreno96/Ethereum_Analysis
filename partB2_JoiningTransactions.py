
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

#http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1574171293853_3638/
class partB2_JoiningTransactions(MRJob):

    def mapper(self, _, line):
        if (len(line.split("\t"))==2): #if it is coming from the output of job1
            fields = line.split("\t") #split by tab
            address = fields[0][1:-1] #remove double quotes ("")
            value = int(fields[1])
            yield (address,(value,1)) #1 as an identifier  

        elif(len(line.split(","))==5): #if it is from the transactions dataset
            if not(line.startswith("address")): #make sure it is not the row with headers
                fields = line.split(",") #split by comma
                address = fields[0]
                yield (address,(None,2)) #2 as an identifier


    def reducer(self, address, values):
        print_flag = False
        val=0
        for i in values:
            if i[1]==2: #if it is comming from the contracts dataset
                print_flag=True
            elif i[1]==1:
                val = int(i[0]) #get the value

        if (print_flag and val>0): #if it is a valid contract and it is bigger than 0
            yield(address,val)
        print_flag=False
        val=0

if __name__ == '__main__':
    partB2_JoiningTransactions.run()
