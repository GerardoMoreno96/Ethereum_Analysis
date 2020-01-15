
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

class ScamAnalysis(MRJob):

    def mapper(self, _, line):
        fields = line.split("},") #split json by curly brace char
        count=0
        for i in range(len(fields)):
            scam = fields[i].split(",") 
            if count is 0: #if i'm reading the first scam in the json file
                address = scam[1][11:54][0:-1] 
                category = scam[5][12:-1]
                status = scam[10][10:-1]
                print("{},{},{}".format(address,category,status))
                count+=1 #add 1 so I know I'm done with the first scam of the json
            else:
                address2 = scam[0][1:].split("\"", 1)[0]
                for i in range(len(scam)):
                    if scam[i].startswith("\"category\""):
                        category2=scam[i][12:-1]
                    elif scam[i].startswith("\"status\""):
                        status2= scam[i].split("\"", 3)[3].split("\"", 1)[0]
                print("{},{},{}".format(address2,category2,status2))

if __name__ == '__main__':
    ScamAnalysis.run()
