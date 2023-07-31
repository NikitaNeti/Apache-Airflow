from pyspark.sql import SparkSession
from pyspark import SparkConf

# Set the driver class path
spark_jars_packages = "/home/neosoft/Videos/mysql-connector-j-8.0.33.jar"
spark_conf = SparkConf().set("spark.driver.extraClassPath", spark_jars_packages)

# Create the SparkSession with the configured SparkConf
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Rest of your code
jdbc_url = "jdbc:mysql://localhost:3306/my_db"
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}
table_name = 'Customers'
df = spark.read.jdbc(jdbc_url, table_name, properties=properties)

table_name = 'Customers'
df1 = spark.read.jdbc(jdbc_url, table_name, properties=properties)
print(df1.show())

table_name = 'Orders'
df2 = spark.read.jdbc(jdbc_url, table_name, properties=properties)

filtered_df = df1.filter(df1.age > 30)
print(filtered_df.show())

output_path = "/home/neosoft/Documents/Assignment_airflow/result.csv" 

spark.stop()

#filtered_df.write.csv(output_path, header=True)