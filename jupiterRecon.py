#!/usr/bin/env python
# coding: utf-8

# In[86]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, expr
from datetime import datetime, timedelta
import os
os.environ['HADOOP_HOME'] = 'C:\\Users\\holde\\hadoop\\winutils-master\\winutils-master\\hadoop-3.0.0'


# In[87]:


postgres_url = "jdbc:postgresql://localhost:5432/ods"
postgres_user = "postgres"
postgres_password = "postgres"
postgres_db = "ods_transaction"
closing_date = "2023-01-01"


# In[88]:


spark = SparkSession.builder     .appName("PostgreSQL Comparison")     .config("spark.jars", "C:/Program Files (x86)/PostgreSQL/pgJDBC/postgresql-42.6.0.jar")     .getOrCreate()


# In[89]:


postgres_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    "sslmode": "prefer",
    "dbname": postgres_db
}


# In[90]:


closing_date = datetime.strptime("2023-01-02", '%Y-%m-%d')


# In[91]:


opening_date = datetime.strptime("2023-01-01", '%Y-%m-%d')


# In[92]:


position_query_open = f"(SELECT as_of_date, account_no,security_no, sum(quantity) as quantity FROM ods_transaction.position WHERE as_of_date in ('{opening_date}') group by as_of_date, account_no,security_no) as position"
position_query_close = f"(SELECT as_of_date, account_no,security_no, sum(quantity) as quantity FROM ods_transaction.position WHERE as_of_date in ('{closing_date}') group by as_of_date, account_no,security_no) as position"
transaction_query_close = f"(SELECT as_of_date, account_no,security_no, sum(quantity) as quantity FROM  ods_transaction.transaction WHERE as_of_date = '{closing_date}' group by as_of_date, account_no,security_no) as transaction"


# In[93]:


print(position_query_open)


# In[94]:


position_df_open = spark.read     .jdbc(url=postgres_url, table=position_query_open, properties=postgres_properties)

position_df_close = spark.read     .jdbc(url=postgres_url, table=position_query_close, properties=postgres_properties)
transaction_df_close = spark.read     .jdbc(url=postgres_url, table=transaction_query_close, properties=postgres_properties)


# In[95]:


transaction_df_close.head()


# In[96]:


df_aggregated_opening = position_df_open.unionByName(transaction_df_close)     .groupBy("account_no", "security_no")     .sum("quantity")     .withColumnRenamed("sum(quantity)", "aggregated_quantity")


# In[97]:


df_aggregated_opening.head(2)


# In[98]:


position_df_close.head()


# In[99]:


position_df_open.head()


# In[100]:


df_breaks = df_aggregated_opening.join(position_df_close, ["account_no", "security_no"], "inner")     .filter(col("aggregated_quantity") != col("quantity"))


# In[101]:


df_breaks.head()


# In[102]:


df_breaks.write     .jdbc(url=postgres_url, table="breaks", mode="overwrite", properties=postgres_properties)


# In[103]:


num_partitions = 20


# In[104]:


position_df_open = position_df_open.withColumn("synthetic_key", expr(f"hash(account_no) % {num_partitions}"))


# In[106]:


position_df_open.head()


# In[ ]:




