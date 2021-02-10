import pandas
import re
import pyspark.sql.functions as F
from pyspark.sql.types import *

def Pgmname(x):

    m = re.search("PROGRAM-ID. (\w+)", x)
    if m is None:
        print("")
    else:
        return (m.group(1))

udffunc = F.udf(Pgmname, returnType=StringType())

