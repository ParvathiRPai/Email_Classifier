# Databricks notebook source
# List's files
display(dbutils.fs.ls("abfss://pava@uwbigdatatechnologies.dfs.core.windows.net/"))


# COMMAND ----------

dbutils.fs.ls("abfss://pava@uwbigdatatechnologies.dfs.core.windows.net/maildir 3/")

# COMMAND ----------

# Loding files from storage explorer
df=spark.read.option("header",True).option("inferSchema",True).text("abfss://pava@uwbigdatatechnologies.dfs.core.windows.net/Assignment-1/maildir/*/*/*")


# COMMAND ----------

# Parsing data frame
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from datetime import datetime

def parse_message(m):
    message_lines = m.split('\n')[:12]
    message_id ='' 
    message_date = '' 
    message_to = '' 
    message_from = ''
    for line in message_lines:
        try:
            if line.startswith("Message-ID: "):
                message_id = ":".join(line.split(":")[1:]).strip()
            if line.startswith("Date: "):
                message_date = ":".join(line.split(":")[1:]).strip()
            if line.startswith("From:"):
                message_from = ":".join(line.split(":")[1:]).strip()
            if line.startswith("To: "):
                message_to = ":".join(line.split(":")[1:]).strip()       
        except Exception as e: 
            print("Exception".format(e, line))

    return (message_id, message_date, message_to, message_from)

schema = StructType([
  StructField("MessageId", StringType(), True),
  StructField("MessageDate", StringType(), True),
  StructField("MessageTo", StringType(), True),
  StructField("MessageFrom", StringType(), True),
#   StructField("MessageBody", StringType(), True),
])

parseMessageUdf = udf(
    lambda s: parse_message(s),
    schema
)

# COMMAND ----------

# parsing each line of data frame with parse_messagefunction
dftemp2=df.withColumn("parsedMessage", parseMessageUdf(df["value"]))



# COMMAND ----------

dftemp3= dftemp2 \
.withColumn("MessageId", dftemp2["parsedMessage.MessageId"]) \
.withColumn("MessageDate", dftemp2["parsedMessage.MessageDate"]) \
.withColumn("MessageTo", dftemp2["parsedMessage.MessageTo"]) \
.withColumn("MessageFrom", dftemp2["parsedMessage.MessageFrom"]) 
# .withColumn("MessageBody", dftemp2["parsedMessage.MessageBody"])
# adding message body is slowing the process

# COMMAND ----------


messageId=dftemp3.where(dftemp3.value.contains("Message-ID:"))
date=dftemp3.where(dftemp3.value.contains("Date: "))
fromsender=dftemp3.where(dftemp3.value.contains("From: "))
tosender=dftemp3.where(dftemp3.value.contains("To: "))

# COMMAND ----------


df1=messageId.select("MessageId")
df2=date.select("MessageDate")
df3=tosender.select("MessageTo")
df4=fromsender.select("MessageFrom")



# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id 
df1 = df1.select("MessageId").withColumn("id", monotonically_increasing_id())
df2 = df2.select("MessageDate").withColumn("id", monotonically_increasing_id())
df3 = df3.select("MessageTo").withColumn("id", monotonically_increasing_id())
df4 = df4.select("MessageFrom").withColumn("id", monotonically_increasing_id())

# COMMAND ----------

res1=df1.join(df2, on="id")
res2=df3.join(df4, on="id")
res=res1.join(res2, on="id")

# COMMAND ----------


res.coalesce(1).write.option("header", True).option("quoteAll", True).csv('abfss://pava@uwbigdatatechnologies.dfs.core.windows.net/Output/result3.csv')

# COMMAND ----------

