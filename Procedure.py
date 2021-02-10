import pandas
import re
import pyspark.sql.functions as F
from pyspark.sql.types import *

def Add(x):
    if "ADD" in x:
        y = x.split('GIVING')
        m = re.sub(r"TO", " ", y[0])
        m = re.search("ADD (.*)", m)
        if m is None:
            print("")
        else:
            return(m.group(1))
            if "GIVING" in x:
                x = "ADD A TO B GIVING C."
                n = re.search("GIVING (.*)", x)
                if m is None:
                    print("")
                else:
                    return(n.group(1))
    else:
        print("")

def Subtract(x):
    if "SUBTRACT" in x:
        y = x.split('GIVING')
        m = re.sub(r"FROM", " ", y[0])
        m = re.search("SUBTRACT (.*)", m)
        if m is None:
            print("")
        else:
            return(m.group(1))
            if "GIVING" in x:
                n = re.search("GIVING (.*)", x)
                if n is None:
                    print("")
                else:
                    return(n.group(1))
    else:
        print("")

def Multiply(x):
    if "MULTIPLY" in x:
        y = x.split('GIVING')
        m = re.sub(r"BY", " ", y[0])
        m = re.search("MULTIPLY (.*)", m)
        if m is None:
            print("")
        else:
            return(m.group(1))
            if "GIVING" in x:
                n = re.search("GIVING (.*)", x)
                if n is None:
                    print("")
                else:
                    return(n.group(1))
    else:
        print("")

def Divide(x):
    if "DIVIDE" in x:
        y = x.split('GIVING')
        m = re.sub(r"BY|INTO", " ", y[0])
        m = re.search("DIVIDE (.*)", m)
        if m is None:
            print("")
        else:
            return(m.group(1))
            if "GIVING" in x:
                n = re.search("GIVING (.*)", x)
                if n is None:
                    print("")
                else:
                    return(n.group(1))
    else:
        print("")

def Compute(x):
    if "COMPUTE" in x:
        m = re.sub(r"ROUNDED", " ", x)
        m = re.search("COMPUTE (.*)", m)
        if m is None:
            print("")
        else:
            return(m.group(1))
    else:
        print("")

def Openmode(x):
    if "PERFORM" not in x and "DISPLAY" not in x:
        m = re.search("OPEN (.*)",x)
        if m is None:
            print("")
        else:
            return m.group(1)
    else:
        print("")


def Closefile(x):
    if "PERFORM" not in x and "DISPLAY" not in x:
        m = re.search("CLOSE (.*)", x)
        if m is None:
            print("")
        else:
            return m.group(1)
    else:
        print("")

def Readrecord(x):
    result=[]
    if 'NEXT INTO' in x:
        y = x.split('NEXT INTO')
        m = re.search("READ (.*?) ", y[0])
        result.append(m.group(1), y[1])
    elif 'READ' in x:
            x = x + "  "
            m = re.search("READ (.*) ", x)
            if m is None:
                print("")
            else:
                result.append(m.group(1))
    else:
        print("")


def Writerecord(x):
    if "PERFORM" not in x and "DISPLAY" not in x:
        m = re.search("WRITE (.*)", x)
        if m is None:
            print("")
        else:
            return m.group(1)
    else:
        print("")

def Rewriterecord(x):
    if "PERFORM" not in x and "DISPLAY" not in x:
        m = re.search("REWRITE (.*)", x)
        if m is None:
            print("")
        else:
            return m.group(1)
    else:
        print("")


def Deleterecord(x):
    if "PERFORM" not in x and "DISPLAY" not in x:
        m = re.search("DELETE (.*)", x)
        if m is None:
            print("")
        else:
            return m.group(1)
    else:
        print("")

def Updaterecord(x):
    if "PERFORM" not in x and "DISPLAY" not in x:
        m = re.search("UPDATE (.*)", x)
        if m is None:
            print("")
        else:
            return m.group(1)
    else:
        print("")

udfaddfunc = F.udf(Add, returnType=StringType())
udfsubfunc = F.udf(Subtract, returnType=StringType())
udfmulfunc = F.udf(Multiply, returnType=StringType())
udfdivfunc = F.udf(Divide, returnType=StringType())
udfcompfunc = F.udf(Compute,returnType=StringType())
udfopenfunc = F.udf(Openmode, returnType=StringType())
udfclosefunc = F.udf(Closefile, returnType=StringType())
udfreadfunc = F.udf(Readrecord, returnType=StringType())
udfwritefunc = F.udf(Writerecord, returnType=StringType())
udfrewritefunc = F.udf(Rewriterecord, returnType=StringType())
udfdeletefunc = F.udf(Deleterecord, returnType=StringType())
udfupdatefunc = F.udf(Updaterecord, returnType=StringType())


