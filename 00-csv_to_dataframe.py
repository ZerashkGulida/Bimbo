from pyspark.sql.types import *

def csv2df_events(sqlCtx):
    common_fields = [
        StructField("Week", IntegerType(), False),
        StructField("Agency_ID", IntegerType(), False),
        StructField("Channel_ID", IntegerType(), False),
        StructField("Route_ID", IntegerType(), False),
        StructField("Client_ID", IntegerType(), False),
        StructField("Product_ID", IntegerType(), False)]

    print "converting train.csv"
    
    train_fields = list(common_fields)
    train_fields.append(StructField("Sales_unit", IntegerType(), True))
    train_fields.append(StructField("Sales_pesos", IntegerType(), False))
    train_fields.append(StructField("Returns_unit", IntegerType(), False))
    train_fields.append(StructField("Returns_pesos", IntegerType(), False))
    train_fields.append(StructField("Adjusted_demand", IntegerType(), False))
    train_schema = StructType(train_fields)
    traindf = sqlCtx.read.csv("new_train.csv", schema=train_schema, header=True, mode='FAILFAST', nullValue="")
    traindf.printSchema()
    traindf.write.parquet("data2/train.parquet")
    
    test_fields = list(common_fields)
    test_fields.append(StructField("id", IntegerType(), True))
    test_schema = StructType(test_fields)
    traindf = sqlCtx.read.csv("new_test.csv", schema=train_schema, header=True, mode='FAILFAST', nullValue="")
    testdf.printSchema()
    testdf.write.parquet("data2/test.parquet")
    

    print "done"


def main():
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    sc = SparkContext()
    sqlCtx = SQLContext(sc)
    csv2df_events(sqlCtx)
    sc.stop()

if __name__ == '__main__':
    main()

