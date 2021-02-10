import pandas
import re
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext

# x = "10 DC-PID               PIC S9(5)V OCCURS 10 TIMES INDEXED BY I            00170001"
# if "OCCURS" in x:
#     m = re.search("OCCURS (\w+) ", x)
#     print(m.group(1))
#     if "INDEXED BY" in x:
#         m = re.search("INDEXED BY (.*) ", x)
#         print(m.group(1))
#
# y = "PERFORM PARA-OPEN-FILE THRU PARA-OPEN-FILE-EXIT. "
# m = re.search("PERFORM (.*?) ", y)
# print(m.group(1))
# if "THRU" in y:
#     m = re.search("THRU (.*) ", y)
#     print(m.group(1))


lines = "C:\\Users\\M1055990\\Desktop\\sample.txt"
conf = SparkConf().setAppName("read text file in pyspark")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

lines = sc.textFile(lines)
list = lines.collect()
llist = lines.zipWithIndex().collect()

mySchema = StructType([StructField("Content", StringType(), True),
                           StructField("Index", IntegerType(), True)])

df = sqlContext.createDataFrame(llist,schema=mySchema)

def perform(x):
    result = []
    if "PERFORM" in x:
        m = re.search("PERFORM (.*?) ", x)
        # return(m.group(1))
        if m is None:
            print("")
        else:
            result.append(m.group(1))
        if "THRU" in x:
            n = re.search("THRU (.*) ", x)
            # return(n.group(1))
            if n is None:
                print("")
            else:
                result.append(n.group(1))
    else:
        print("")
    return result

udfperform = F.udf(perform, returnType=StringType())
df1 = df.filter(df.Content.contains("PERFORM"))
df1 = df1.withColumn("Perform_para", udfperform("Content"))
df1 = df1.select(["Perform_para"]).collect()

for para in df1:
    print(para.Perform_para)
    # print(para[1].Perform_para)

df_evaluate = df.filter(df.Content.contains("EVALUATE" or "END-EVALUATE"))
eval_list = df_evaluate.select("Index").collect()
result = []
result1 = []
i = 0
while (i < len(eval_list)):
        j = i + 1
        m = eval_list[i].Index
        n = eval_list[j].Index
        el = df.filter(df['Index'].between(m,n))
        el = el.collect()
        for line in el:
            result.append(line.Content)
        result1.append(result)
        i = i + 2

print(result1)
df2 = sqlContext.createDataFrame(result1,StringType())
df2.show()