import re
import pyspark

sc = pyspark.SparkContext()

def is_good_line_transactions(line):
    try:
        fields = line.split(",")
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        return False

def is_good_line_contracts(line):
    try:
        fields = line.split(",")
        if len(fields) != 5:
            return False
        float(fields[3])
        return True
    except:
        return False

lines_contracts = sc.textFile('hdfs://andromeda.student.eecs.qmul.ac.uk/data/ethereum/contracts')
filtered_lines_contracts = lines_contracts.filter(is_good_line_contracts)
address = filtered_lines_contracts.map(lambda l:(l.split(",")[0],1))

lines_transactions = sc.textFile('hdfs://andromeda.student.eecs.qmul.ac.uk/data/ethereum/transactions')
filtered_lines_transactions = lines_transactions.filter(is_good_line_transactions)
address_val_pair = filtered_lines_transactions.map(lambda l: (l.split(",")[2],float(l.split(",")[3])))
results = address_val_pair.join(address)
address_val_pair_reduced = results.reduceByKey(lambda (a,b), (c,d): (float(a) + float(c), b+d))

topten = address_val_pair_reduced.takeOrdered(10, key = lambda x: -x[1][0])

for i in topten:
    print(i)