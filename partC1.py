
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class partC1(MRJob):

    def mapper(self, _, line):
        if (len(line.split(","))==3): #check if the input is from the csv
            fields = line.split(",")
            address = fields[0]
            category = fields[1]
            yield (address,(category,1))

        elif(len(line.split("\t"))==2): #check if the input is from the tsv
            fields = line.split("\t")
            address = fields[0][1:-1]
            value = fields[1]
            yield (address,(value,2))

    def reducer(self, address, values):
        print_flag = False
        val=0
        category=""
        for i in values:
            if i[1]==1:
                print_flag=True
                category = i[0]
            elif i[1]==2:
                val = int(i[0])

        if (print_flag and val>0):
            yield(category,val)
        print_flag=False
        val=0

if __name__ == '__main__':
    partC1.run()
