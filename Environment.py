import pandas
import re
import pyspark.sql.functions as F
from pyspark.sql.types import *

def filename(x):
    m = re.search("SELECT (.*) ", x)
    if m is None:
        print("")
    else:
        return m.group(1)

def Organization(x):
    m = re.search("ORGANIZATION IS (\w+)", x)
    if m is None:
        print("")
    else:
        return m.group(1)

def Accessmode(x):
    m = re.search("ACCESS MODE IS (\w+)", x)
    if m is None:
        print("")
    else:
        return m.group(1)

def Filestatus(x):
    m = re.search("FILE STATUS IS (.*)", x)
    if m is None:
        print("")
    else:
        return m.group(1)

def Keyksds(x):
    if "RECORD KEY   IS" in x:
        m = re.search("RECORD KEY IS (.*)", x)
        if m is None:
            print("")
        else:
            return m.group(1)
    else:
        print("")



udffilefunc = F.udf(filename, returnType=StringType())
udforgfunc = F.udf(Organization, returnType=StringType())
udfaccessfunc = F.udf(Accessmode, returnType=StringType())
udfstatusfunc = F.udf(Filestatus, returnType=StringType())
udfkeyfunc = F.udf(Keyksds,returnType=StringType())