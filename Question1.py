import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr,sum as spark_sum,avg,count,when
import sqlite3
import pandas as pd 


spark=SparkSession.builder.appName("SalesData") .getOrCreate()
def read_data(file_path):
    df=spark.read.csv(file_path,header=True,inferSchema=True)
    df.printSchema()
    return df


def trans_data(df,region):
    df=df.withColumn('region',expr(f"'{region}'"))
    df=df.withColumn('total_sales',col('QuantityOrdered')*col('ItemPrice'))
    df=df.withColumn('net_sales',col('total_sales')-col('PromotionDiscount'))
    df=df.filter(col('net_sales')>0)

    return df


def load_data(df,db_name,table_name):
    pandas_df = df.toPandas()
    conn=sqlite3.connect(db_name)
    pandas_df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def run_sql_queries(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sales_data")
    total_records = cursor.fetchone()[0]
    print(f"Total number of records: {total_records}")

    cursor.execute("SELECT region, SUM(total_sales) FROM sales_data GROUP BY region")
    total_sales_by_region = cursor.fetchall()
    print(f"Total sales by region: {total_sales_by_region}")

    cursor.execute("SELECT AVG(net_sales) FROM sales_data")
    avg_sales_per_transaction = cursor.fetchone()[0]
    print(f"Average sales amount per transaction: {avg_sales_per_transaction}")

    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT OrderId, COUNT(*) 
            FROM sales_data 
            GROUP BY OrderId 
            HAVING COUNT(*) > 1
        ) AS duplicates
    """)
    duplicate_orders = cursor.fetchone()[0]
    if duplicate_orders == 0:
        print("No duplicate OrderId values found.")
    else:
        print(f"{duplicate_orders} duplicate OrderId values found.")

    conn.close()

def main():
    region_a_file = "order_region_a(in).csv"
    region_b_file = "order_region_b(in).csv"
    
    region_a_data = read_data(region_a_file)
    region_b_data = read_data(region_b_file)
    
    transformed_region_a = trans_data(region_a_data, "A")
    transformed_region_b = trans_data(region_b_data, "B")
    
    combined_data = transformed_region_a.union(transformed_region_b).dropDuplicates(['OrderId'])
    
    load_data(combined_data, "sales_data.db", "sales_data")
    
    run_sql_queries("sales_data.db")

if __name__ == "__main__":
    main()


