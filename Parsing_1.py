import pandas
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext
import re
from pyspark.sql.types import *
from Identification import *
from Environment import *
from Data_div import *
from Procedure import *

class parsing:
    conf = SparkConf().setAppName("read text file in pyspark")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Read file into RDD
    lines = sc.textFile("CobolCode.txt")

    # Call collect() to get all data and ZipWithIndex for adding index
    # llist = lines.collect()
    llist = lines.zipWithIndex().collect()
    # print(llist)

    # index = rdd.zipWithIndex().filter(lambda (key,index) : key == "       PROGRAM-ID.").collect()

    mySchema = StructType([StructField("Content", StringType(), True),
                           StructField("Index", IntegerType(), True)])

    df = sqlContext.createDataFrame(llist,schema=mySchema)
    # df.show()

    division = df.filter(df.Content.contains("DIVISION."))
    # division.show()

    # division.select(division.coloums[:2]).show()
    index_list = division.select('Index').collect()
    # print(index_list[0].Index)

    #Indentification dataframe
    i = index_list[1].Index
    Identification_df = df.filter(df['Index']<i)
    # Identification_df.show()

    #Environment dataframe
    j = index_list[2].Index
    Environment_df = df.filter(df['Index'].between(i,j-1))

    #Data division dataframe
    line = df.filter(df.Content.contains(" WORKING-STORAGE SECTION."))
    declare = df.filter(df.Content.contains("DECLARE SECTION"))
    Declare_index_list = declare.select('Index').collect()
    ws_index_list = line.select('Index').collect()
    j = index_list[2].Index
    k = index_list[3].Index
    m = ws_index_list[0].Index
    if len(Declare_index_list) >0:
        n = Declare_index_list[0].Index
        p = Declare_index_list[1].Index
        Declare_Data_df = df.filter(df['Index'].between(n + 2, p))
    # print(m,k,n,p)
    File_Data_df = df.filter(df['Index'].between(j, m - 1))
    WS_Data_df = df.filter(df['Index'].between(m, k - 1))



    #Procedure Division
    Procedure_df = df.filter(df['Index'] >= k)
    # Procedure_df.show()

    #Defining Excel sheet
    writer = pandas.ExcelWriter("Parsing_df_cobol.xls",
                                engine='xlsxwriter')

    pdf = df.toPandas()
    pdf.to_excel(writer, sheet_name='Cobol code', index=False)

    ID_df = Identification_df.withColumn("Pgm Name",udffunc("Content"))
    ID_df = ID_df.select(["Pgm Name"])
    ID_df = ID_df.dropna()
    ID_pdf = ID_df.toPandas()
    # print(ID_pdf)
    ID_pdf.to_excel(writer, sheet_name='Identification output', index=False)


    # Environment Division segregation
    envi_filedf = Environment_df.withColumn("File Name", udffilefunc("Content"))
    envi_filedf = envi_filedf.select(["File Name"])
    envi_filepdf = envi_filedf.toPandas()
    envi_filepdf = envi_filepdf.dropna()
    envi_orgdf = Environment_df.withColumn("Organization", udforgfunc("Content"))
    envi_orgdf = envi_orgdf.select(["Organization"])
    envi_orgpdf = envi_orgdf.toPandas()
    envi_orgpdf = envi_orgpdf.dropna()
    envi_accessdf = Environment_df.withColumn("Access", udfaccessfunc("Content"))
    envi_accessdf = envi_accessdf.select(["Access"])
    envi_accesspdf = envi_accessdf.toPandas()
    envi_accesspdf = envi_accesspdf.dropna()
    envi_statusdf = Environment_df.withColumn("Status", udfstatusfunc("Content"))
    envi_statusdf = envi_statusdf.select(["Status"])
    envi_statuspdf = envi_statusdf.toPandas()
    envi_statuspdf = envi_statuspdf.dropna()
    envi_keydf = Environment_df.withColumn("Key Value", udfstatusfunc("Content"))
    envi_keydf = envi_keydf.select(["Key Value"])
    envi_keypdf = envi_keydf.toPandas()
    envi_keypdf = envi_keypdf.dropna()
    envi_df = pandas.concat([envi_filepdf,envi_orgpdf,envi_accesspdf,envi_statuspdf,envi_keypdf],axis=1)

    envi_df.to_excel(writer, sheet_name='Environment output', index=False)

    # Data Division segregation
    datavar_df = File_Data_df.withColumn("FD Variable", udfvarfunc("Content"))
    datavar_df = datavar_df.select(["FD Variable"])
    datavar_pdf = datavar_df.toPandas()
    datavar_pdf = datavar_pdf.dropna()
    datadec_df = File_Data_df.withColumn("FD DataDeclaration", udfdatatypefunc("Content"))
    datadec_df = datadec_df.select(["FD DataDeclaration"])
    datadec_pdf = datadec_df.toPandas()
    datadec_pdf  = datadec_pdf .dropna()
    datatype_df = datadec_df.withColumn("FD DataType", udfdataallfunc("FD DataDeclaration"))
    datatype_df = datatype_df.select(["FD DataType"])
    datatype_pdf = datatype_df.toPandas()
    datatype_pdf = datatype_pdf.dropna()
    datacomp_df = File_Data_df.withColumn("FD Comp value", udfcompvalfunc("Content"))
    datacomp_df = datacomp_df.select(["FD Comp value"])
    datacomp_pdf = datacomp_df.toPandas()
    datacomp_pdf = datacomp_pdf.dropna()
    dataval_df = File_Data_df.withColumn("FD Value Clause", udfdataallfunc("Content"))
    dataval_df = dataval_df.select(["FD Value Clause"])
    dataval_pdf = dataval_df.toPandas()
    dataval_pdf = dataval_pdf.dropna()
    data_df = pandas.concat([datavar_pdf,datadec_pdf,datatype_pdf,datacomp_pdf,dataval_pdf],axis=1)
    data_df = data_df.dropna()
    # print(data_df)

    ws_datavar_df = WS_Data_df.withColumn("WS Variable", udfvarfunc("Content"))
    ws_datavar_df = ws_datavar_df.select(["WS Variable"])
    ws_datavar_pdf = ws_datavar_df.toPandas()
    ws_datavar_pdf = ws_datavar_pdf.dropna()
    # print(ws_datavar_pdf)
    ws_datadec_df = WS_Data_df.withColumn("WS DataDeclaration", udfdatatypefunc("Content"))
    ws_datadec_df = ws_datadec_df.select(["WS DataDeclaration"])
    ws_datadec_pdf = ws_datadec_df.toPandas()
    ws_datadec_pdf = ws_datadec_pdf.dropna()
    # print(ws_datadec_pdf)
    ws_datatype_df = ws_datadec_df.withColumn("WS DataType", udfdataallfunc("WS DataDeclaration"))
    ws_datatype_df = ws_datatype_df.select(["WS DataType"])
    ws_datatype_pdf = ws_datatype_df.toPandas()
    ws_datatype_pdf = ws_datatype_pdf.dropna()
    ws_datacomp_df = WS_Data_df.withColumn("WS Comp value", udfcompvalfunc("Content"))
    ws_datacomp_df = ws_datacomp_df.select(["WS Comp value"])
    ws_datacomp_pdf = ws_datacomp_df.toPandas()
    ws_datacomp_pdf = ws_datacomp_pdf.dropna()
    ws_dataval_df = File_Data_df.withColumn("WS Value Clause", udfdataallfunc("Content"))
    ws_dataval_df = ws_dataval_df.select(["WS Value Clause"])
    ws_dataval_pdf = ws_dataval_df.toPandas()
    ws_dataval_pdf = ws_dataval_pdf.dropna()
    # print(ws_datatype_pdf)
    ws_data_df = pandas.concat([ws_datavar_pdf, ws_datadec_pdf,ws_datatype_pdf,ws_datacomp_pdf,ws_dataval_pdf],axis=1)
    ws_data_df = ws_data_df.dropna()
    # print(ws_data_df)

    if len(Declare_index_list) >0:
        De_datavar_df = Declare_Data_df.withColumn("DB2 Variable", udfvarfunc("Content"))
        De_datavar_df = De_datavar_df.select(["DB2 Variable"])
        De_datavar_pdf = De_datavar_df.toPandas()
        De_datavar_pdf = De_datavar_pdf.dropna()
        De_datadec_df = Declare_Data_df.withColumn("DB2 DataDeclaration", udfdatatypefunc("Content"))
        De_datadec_df = De_datadec_df.select(["DB2 DataDeclaration"])
        De_datadec_pdf = De_datadec_df.toPandas()
        De_datadec_pdf = De_datadec_pdf.dropna()
        De_datatype_df = De_datadec_df.withColumn("DB2 DataType", udfdataallfunc("DB2 DataDeclaration"))
        De_datatype_df = De_datatype_df.select(["DB2 DataType"])
        De_datatype_pdf = De_datatype_df.toPandas()
        De_datatype_pdf = De_datatype_pdf.dropna()
        De_datacomp_df = File_Data_df.withColumn("DB2 Comp value", udfcompvalfunc("Content"))
        De_datacomp_df = De_datacomp_df.select(["DB2 Comp value"])
        De_datacomp_pdf = De_datacomp_df.toPandas()
        De_datacomp_pdf = De_datacomp_pdf.dropna()
        De_dataval_df = File_Data_df.withColumn("DB2 Value Clause", udfdataallfunc("Content"))
        De_dataval_df = De_dataval_df.select(["DB2 Value Clause"])
        De_dataval_pdf = De_dataval_df.toPandas()
        De_dataval_pdf = De_dataval_pdf.dropna()
        De_data_df = pandas.concat([De_datavar_pdf, De_datadec_pdf, De_datatype_pdf,De_datacomp_pdf,De_dataval_pdf],axis=1)
        De_data_df = De_data_df.dropna()
        De_data_df = pandas.concat([data_df, ws_data_df, De_data_df], axis=1)
        De_data_df.to_excel(writer, sheet_name='Data output', index=False)

    if len(Declare_index_list) <= 0:
        De_data_df = pandas.concat([data_df, ws_data_df,], axis=1)
        De_data_df.to_excel(writer, sheet_name='Data output', index=False)

    # Producer Division segregation
    prod_adddf = Procedure_df.withColumn("Addvar", udfaddfunc("Content"))
    prod_adddf = prod_adddf.select(["Addvar"])
    prod_add_pdf = prod_adddf.toPandas()
    prod_add_pdf = prod_add_pdf.dropna()
    prod_subdf = Procedure_df.withColumn("Subvar", udfsubfunc("Content"))
    prod_subdf =  prod_subdf.select(["Subvar"])
    prod_sub_pdf = prod_subdf.toPandas()
    prod_sub_pdf =  prod_sub_pdf.dropna()
    prod_muldf = Procedure_df.withColumn("Mulvar", udfmulfunc("Content"))
    prod_muldf = prod_muldf.select(["Mulvar"])
    prod_mul_pdf = prod_muldf.toPandas()
    prod_mul_pdf = prod_mul_pdf.dropna()
    prod_divdf = Procedure_df.withColumn("Divvar", udfdivfunc("Content"))
    prod_divdf = prod_divdf.select(["Divvar"])
    prod_div_pdf = prod_divdf.toPandas()
    prod_div_pdf = prod_div_pdf.dropna()
    prod_compdf = Procedure_df.withColumn("Compeq", udfcompfunc("Content"))
    prod_compdf = prod_compdf.select(["Compeq"])
    prod_comp_pdf = prod_compdf.toPandas()
    prod_comp_pdf = prod_comp_pdf.dropna()
    prod_opendf = Procedure_df.withColumn("OpenMode", udfopenfunc("Content"))
    prod_opendf = prod_opendf.select(["OpenMode"])
    prod_open_pdf = prod_opendf.toPandas()
    prod_open_pdf = prod_open_pdf.dropna()
    prod_closedf = Procedure_df.withColumn("Closerec", udfclosefunc("Content"))
    prod_closedf = prod_closedf.select(["Closerec"])
    prod_close_pdf = prod_closedf.toPandas()
    prod_close_pdf = prod_close_pdf.dropna()
    prod_readdf = Procedure_df.withColumn("Readrec", udfreadfunc("Content"))
    prod_readdf = prod_readdf.select(["Readrec"])
    prod_read_pdf = prod_readdf.toPandas()
    prod_read_pdf = prod_read_pdf.dropna()
    prod_writedf = Procedure_df.withColumn("Writerec", udfwritefunc("Content"))
    prod_writedf = prod_writedf.select(["Writerec"])
    prod_write_pdf = prod_writedf.toPandas()
    prod_write_pdf = prod_write_pdf.dropna()
    prod_rewritedf = Procedure_df.withColumn("Rewriterec", udfrewritefunc("Content"))
    prod_rewritedf = prod_rewritedf.select(["Rewriterec"])
    prod_rewrite_pdf = prod_rewritedf.toPandas()
    prod_rewrite_pdf = prod_rewrite_pdf.dropna()
    prod_deldf = Procedure_df.withColumn("Deleterec", udfdeletefunc("Content"))
    prod_deldf = prod_deldf.select(["Deleterec"])
    prod_del_pdf = prod_deldf.toPandas()
    prod_del_pdf = prod_del_pdf.dropna()
    prod_updatedf = Procedure_df.withColumn("Updaterec", udfupdatefunc("Content"))
    prod_updatedf = prod_updatedf.select(["Updaterec"])
    prod_update_pdf = prod_updatedf.toPandas()
    prod_update_pdf = prod_update_pdf.dropna()
    prod_df = pandas.concat([prod_add_pdf,prod_sub_pdf,prod_mul_pdf,prod_div_pdf,prod_comp_pdf,prod_open_pdf, \
                             prod_close_pdf,prod_read_pdf,prod_write_pdf,prod_rewrite_pdf,prod_del_pdf,prod_update_pdf], axis=1)

    prod_df.to_excel(writer, sheet_name='Procedure output', index=False)

    Exec = WS_Data_df.filter(WS_Data_df.Content.contains("EXEC"))
    Exec_list = Exec.select('Index').collect()
    print(len(Exec_list))
    i = 0
    result = []
    result1 = []
    while (i < len(Exec_list)):
        j = i + 1
        m = Exec_list[i].Index
        n = Exec_list[j].Index
        el = df.filter(df['Index'].between(m,n))
        # exec_df.show()
        eli = el.collect()
        for data in eli:
            # print(data.Content)
            if 'INCLUDE' in data.Content:
                m = re.search("INCLUDE (\w+)", data.Content)
                result.append(m.group(1))
            elif "DECLARE" in data.Content:
                m = re.search("DECLARE (\w+)", data.Content)
                result.append(m.group(1))
                if "CURSOR FOR" in data.Content:
                    print(data.Index)
                    print(data.Content)
            else:
                print()

        i = i + 2

    print(result)


    Exec = Procedure_df.filter(Procedure_df.Content.contains("EXEC"))
    Exec_list = Exec.select('Index').collect()
    print(Exec_list)
    elist = []
    i = 0
    while (i < len(Exec_list)):
        k = 0
        j = i + 1
        m = Exec_list[i].Index
        n = Exec_list[j].Index
        el = df.filter(df['Index'].between(m + 1, n - 1))
        eli = el.collect()
        for data in eli:
            result1.append(data.Content)
            if "INTO" in data.Content:
                m = re.search("INTO (.*)", data.Content)
                result1.append(m.group(1))

        i = i + 2

    print(result1)
    exec_wsdf = pandas.Series(result, name="EXEC Working")
    exec_proddf = pandas.Series(result1, name="EXEC Procedure")

    exec = pandas.concat([exec_wsdf,exec_proddf], axis=1)
    # print(exec)
    exec.to_excel(writer, sheet_name='Exec output', index=False)

    writer.save()

if __name__ == '__main__':
    Parse = parsing()
