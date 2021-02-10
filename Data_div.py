import pandas
import re
import pyspark.sql.functions as F
from pyspark.sql.types import *

def Variables(x):
    m = re.search("[0-9]+ (.*?) \ PIC", x)
    if m is None:
        print("")
    else:
        return m.group(1)

def Datatype(x):
    m = re.search("PIC (\w+)", x)
    if m is None:
        print("")
    else:
        return m.group(1)

def Datatypeall(x):
    if x == '9':
        return "Numeric"
    elif x == 'A':
        return "Alphabet"
    elif x == 'S9':
        return 'Sign'
    elif x == 'X':
        return "Alphanumeric"
    elif x =="Z":
        return "Zeros"
    else:
        print("")

def Compval(x):
    if "COMP" in x:
        return "BINARY FORM"
    elif "COMP-1" in x:
        return "FLOTING FORM"
    elif "COMP-2" in x:
        return "HEXA DECIMAL FORM"
    elif "COMP-3" in x:
        return "PACKED DECIMAL FORM"
    else:
        print("")

def Valueclause(x):
    if "VALUE" in x:
        m = re.search("VALUE (.*)", x)
        return m.group(1)
    else:
        print("")

def Occurs(x):
    result=[]
    if "OCCURS" in x:
        m = re.search("OCCURS (\w+) ", x)
        result.append(m.group(1))
        if "INDEXED BY" in x:
            m = re.search("INDEXED BY (.*) ", x)
            result.append(m.group(1))
        return result
    else:
        print("")

udfvarfunc = F.udf(Variables, returnType=StringType())
udfdatatypefunc = F.udf(Datatype, returnType=StringType())
udfdataallfunc = F.udf(Datatypeall, returnType=StringType())
udfcompvalfunc = F.udf(Compval, returnType=StringType())
udfValueclausefunc = F.udf(Valueclause, returnType=StringType())
udfOccursfunc = F.udf(Occurs, returnType=StringType())
