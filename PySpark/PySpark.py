# Databricks notebook source
# MAGIC %md 
# MAGIC ### Data Reading
# MAGIC
# MAGIC location: workspace.default.bigmartsales <br>
# MAGIC dbutils.fs.ls('/FileStore/Tables') <br>
# MAGIC
# MAGIC /Volumes/workspace/zahidschema/unitycatalogbyzahid/BigMartSales.csv

# COMMAND ----------

# MAGIC %md 
# MAGIC ### CSV 

# COMMAND ----------

## many organization disable it for leakage of data

display(dbutils.fs.ls('/Volumes/workspace/zahidschema/unitycatalogbyzahid/BigMartSales.csv'))

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/workspace/zahidschema/unitycatalogbyzahid/BigMartSales.csv')

# COMMAND ----------

# df.show()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Data Reading JSON

# COMMAND ----------

df_json=spark.read.format('json').option('inferSchema',True)\
                                 .option('header',True)\
                                 .option('multiLine',False)\
                                 .load('/Volumes/workspace/zahidschema/unitycatalogbyzahid/drivers.json')


# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Data Transformation
# MAGIC ## Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT

# COMMAND ----------

df.select("Item_Identifier","Item_Weight","Item_Fat_Content").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## col
# MAGIC

# COMMAND ----------

#df.select(col("Item_Identifier"),col("Item_Weight"),col("Item_Fat_Content")).display()

from pyspark.sql.functions import col

# Select specific columns
df.select(col("Item_Identifier"), col("Item_Weight"), col("Item_Fat_Content")).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##ALIAS

# COMMAND ----------

df.select(col("Item_Identifier").alias("ItemID"), col("Item_Weight").alias("ItemWeight"), col("Item_Fat_Content").alias("ItemFatContent")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FILTER/ WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC **
# MAGIC - filter the data with fat content = Regular
# MAGIC - Slice the data with item type = soft drink and weight <10
# MAGIC - fetch the data with Tier in (Tier1 or Tier 2) & outlet size is Null
# MAGIC **

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC - **FILTER**
# MAGIC - Scenerio 1
# MAGIC

# COMMAND ----------

df.filter(col("Item_Fat_Content")=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sceneio 2
# MAGIC
# MAGIC Slice the data with item type = soft drink and weight <10
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
df.filter((col("Item_Type")=="Soft Drinks") & (col("Item_Weight")<10)).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Scenerio 3
# MAGIC fetch the data with Tier in (Tier1 or Tier 2) & outlet size is Null

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **.isin() <br>
# MAGIC .isnull()**

# COMMAND ----------

from pyspark.sql.functions import col

result = df.filter(
    (col("Outlet_Size").isNull()) &
    (col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))
)

display(result)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## withColumnRenamed

# COMMAND ----------

df.withColumnRenamed("Item_Weight","Item_wt").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## `withColumn`
# MAGIC **Scenerio 1** : new COlumn
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

df = df.withColumn('flag', lit("new"))

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##modify
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
df.withColumn('multiply',col("Item_Weight")*col("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Scenerio 3
# MAGIC ##modify column item

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
df.withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Regular","Reg"))\
    .withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Low Fat","LF"))\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Type casting
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StringType

df1= df.withColumn("Item_Weight",col("Item_Weight").cast(StringType()))



# COMMAND ----------

print(df1.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort / orderby

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### scenerio 1

# COMMAND ----------

df.sort(col("Item_Weight").desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Scenerio 2

# COMMAND ----------

df.sort(col("Item_Visibility").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenerio 3
# MAGIC
# MAGIC sorting based on multiple column

# COMMAND ----------

#df.sort(["Item_Weight","Item_Visibility"].ascending = [0,0]).display()

df.sort(["Item_Weight", "Item_Visibility"], ascending=False).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Scenerio 4
# MAGIC ### sort in ascending in 1 column and descending in another coumn

# COMMAND ----------

df.orderBy(col("Item_Weight").desc(), col("Item_Visibility").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # LIMIT
# MAGIC

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # drop
# MAGIC to remove column

# COMMAND ----------

# MAGIC %md
# MAGIC 1 column remove

# COMMAND ----------

df.drop("Item_Visibility").display()

# COMMAND ----------

# MAGIC %md
# MAGIC sceneio 2
# MAGIC 2 column remove

# COMMAND ----------

df.drop("Item_Visibility","Item_Type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DROP_DUPLICATES

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md 
# MAGIC 2nd scenerio
# MAGIC # particular column

# COMMAND ----------

df.dropDuplicates(subset=["Item_Type"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Distinct()
# MAGIC (unique)

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION
# MAGIC & 
# MAGIC ## UNION BY Name

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing DataFrame**

# COMMAND ----------

data1 =[('1','Kad'),
        (2,'Sid'),
        ('3','rah')]

schema1 = 'id STRING, name STRING'
df1 = spark.createDataFrame(data1,schema1)

data2 =[('3','rah'),
        (4,'zaa')]

schema2 = 'id STRING, name STRING'
df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **UNION**

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Union by name**

# COMMAND ----------

datames1 =[('Kad',1),
        ('Sid',2)]

schemames1 = 'id STRING, name STRING'
dfmes1 = spark.createDataFrame(data1,schemames1)
dfmes1.display()

# COMMAND ----------

dfmes1.union(df2).display()

# COMMAND ----------

dfmes1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # String Functions
# MAGIC ## INITCAP()
# MAGIC ## UPPER()
# MAGIC ## LOWER()

# COMMAND ----------

### INITCAP()


# COMMAND ----------

from pyspark.sql.functions import *
#df.select(initcap('Item_Type')).display()
df.select(initcap("Item_Type").alias("Item_Type_Capitalized")).display()

# COMMAND ----------

#lower
df.select(lower('Item_Type')).display()

# COMMAND ----------

#Upper & Alias

df.select(upper('Item_Type').alias("Item Type Upper")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATE FUNCTIONS
# MAGIC ## Current_Date()
# MAGIC ## DATE_ADD()
# MAGIC ## DATE_SUB()

# COMMAND ----------

# MAGIC %md
# MAGIC ##                    Current_Date()

# COMMAND ----------

df = df.withColumn('cur_date',current_date())
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC **add 1 week**
# MAGIC ## Date_Add()

# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn('week_after', date_add(col('cur_date'), 7))
df.display()

# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn('week_before', date_sub(col('cur_date'), 7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATEDIFF()
# MAGIC TO FIND THE DATE DIFFERENCE 

# COMMAND ----------

df = df.withColumn('datediff (cur - before)',datediff('cur_date','week_before'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATE_FORMAT
# MAGIC Transform the column

# COMMAND ----------

df = df.withColumn('Week_before',date_format('week_before','dd-MM-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling Nulls
# MAGIC
# MAGIC ## Dropping Nulls
# MAGIC ## Filling Nulla
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC **Dropping Nulls**

# COMMAND ----------

#Drop all values which have nulls
#any it drops any cell hve null it drop the whole row
#all it drop if all cell of row is null

df.dropna('all').display()

# COMMAND ----------

# very sensitive

df.dropna('any').display()

# COMMAND ----------

# particular column

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filling NA or nulls
# MAGIC
# MAGIC ## fillna()

# COMMAND ----------

df.fillna('Not Available').display()

# COMMAND ----------

#subset

df.fillna('NotA',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # SPLIT & Indexing
# MAGIC
# MAGIC to make multiple string element as a list, and then indexing

# COMMAND ----------

# MAGIC %md
# MAGIC Split()

# COMMAND ----------

# create list
df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC indexing

# COMMAND ----------

df.withColumn('Outlet_Type type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Explode

# COMMAND ----------

#creaate list and then explodde it
df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

#df_exp.withColumn('Outlet_Type1',explode('Outlet_Type')).display()

from pyspark.sql.functions import explode, split, col

# Step 1: Make sure df_exp is defined and contains the 'Outlet_Type' column
df_exp1 = df.select("Outlet_Type")  # or df_exp if already defined

# Step 2: Split the string into an array and explode it
df_exp2 = df_exp1.withColumn("Outlet_Type1", explode(split(col("Outlet_Type"), " ")))

# Step 3: Display the result
df_exp2.display()  # Use .display() if you're in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC # _Array_Contains_

# COMMAND ----------

df_exxp = df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import array_contains, split, col

df_exxxp = df.withColumn("Type1_flag", array_contains(split(col("Outlet_Type"), " "), "Type1"))
df_exxxp.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # GROUP_BY
# MAGIC
# MAGIC **

# COMMAND ----------

# MAGIC %md
# MAGIC scenerio 1

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

## Scenerio 
## Average


df.groupBy('Item_Type').agg(avg('Item_MRP')).display()


# COMMAND ----------

from pyspark.sql.functions import sum, col

df.groupBy("Item_Type", "Outlet_Size").agg(sum(col("Item_MRP")).alias("Total_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenerio 4
# MAGIC

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # COllECT_LIST
# MAGIC alternative or replacement of GROUPCONCAT from sql
# MAGIC
# MAGIC

# COMMAND ----------

data_book = [('user1','book 1'), ('user1','book 2'), ('user2','book 2'), ('user3','book 3'), ('user3','book 4'), ('user3','book 1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data_book, schema)

df_book.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC collect_list()
# MAGIC

# COMMAND ----------

df_book.groupBy('user').agg(collect_list("book")).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #PIVOT
# MAGIC spark allows us to use pivot of big data

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # WHEN-OTHERWISE
# MAGIC
# MAGIC sql function: CASE WHEN STATEMENT (if else)

# COMMAND ----------

#scenerio 
df_vegFlag = df.withColumn("veg flag",when(col('Item_Type')=="Meat","Non-Veg").otherwise('Veg'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### scenerio 2
# MAGIC item type veg,
# MAGIC price is >100
# MAGIC than veg expensive

# COMMAND ----------

from pyspark.sql.functions import when, col

df_vegFlag = df.withColumn(
    "df_vegFlag",
    when((col("Item_Type") != "Meat") & (col("Item_MRP") < 100), "Veg_Inexpensive")
    .when((col("Item_Type") != "Meat") & (col("Item_MRP") > 100), "Veg_Expensive")
    .otherwise("Non-Veg")
)

df_vegFlag.display()  # Use display(df_vegFlag) if you're in Databricks


# COMMAND ----------

# MAGIC %md 
# MAGIC # JOINS
# MAGIC
# MAGIC ###  - Inner Join
# MAGIC ###  - outer Join
# MAGIC ###  - right join
# MAGIC ###  - full join
# MAGIC ###  - anti join
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **_Inner **JOIN**_**
# MAGIC

# COMMAND ----------

dataj1 = [('1','aura','d01'),
          ('2','min','d02'),
          ('3','sam','d03'),
          ('4','tom','d03'),
          ('5','viv','d05'),
          ('6','xx','d06')]
schemaJ1 = 'emp_id STRING, emp_name STRING, dept_id STRING'
df1 = spark.createDataFrame(dataj1,schemaJ1)

dataj2 =[('d01','HR'),
         ('d02','IT'),
         ('d03','FINANCE'),
         ('d04','MARKETING'),
         ('d05','SALES')]
schemaJ2 = 'dept_id STRING, dept_name STRING'
df2 = spark.createDataFrame(dataj2,schemaJ2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

df1.join(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Inner Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Left Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()


# COMMAND ----------

# MAGIC %md 
# MAGIC # Right JoiN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ANTI JOIN()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()
df2.join(df1,df2['dept_id']==df1['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Window function

# COMMAND ----------

# MAGIC %md
# MAGIC ## ROW NUMBER()

# COMMAND ----------


from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## RANK()
# MAGIC ## DENSE_RANK()

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
  .withColumn('denRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DENSE_RANK()

# COMMAND ----------

df.withColumn('denRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cumulative Sum
# MAGIC (Running Sum)

# COMMAND ----------

# MAGIC %md
# MAGIC sum func + wimdows func<br>
# MAGIC sum() + over() <br>
# MAGIC frame clause
# MAGIC
# MAGIC

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

#it agreegated the itemtype and gave result

# COMMAND ----------

#frame clause
df.withColumn('cumsum1',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

#UNBOUNDED PRECEDING AND FOLLOWING
df.withColumn('TOTALSUM',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # USER DEFINED FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC # USER DEFINED FUNCTION (UDF)

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

#step 2
my_udf =udf(my_func)

#now it is registered in pyspark

# COMMAND ----------

df.withColumn('myNewCol',my_udf(col('Item_MRP'))).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### read -> transform -> write

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing|

# COMMAND ----------

# MAGIC %md
# MAGIC Azure -> ADL Gen 2 (DataLake) <br>
# MAGIC AWS -> S3 <br>
# MAGIC Databrick -> default databrick Storag & csv
# MAGIC

# COMMAND ----------

df.write.format('csv').save('/Volumes/workspace/zahidschema/unitycatalogbyzahid/Data.csv')
 #   .save('/FileStore/tables/CSV/data.csv')
     

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Mode
# MAGIC Append<br>
# MAGIC Overwrite<br>
# MAGIC Error<br>
# MAGIC Ignore

# COMMAND ----------

# MAGIC %md
# MAGIC Append
# MAGIC

# COMMAND ----------

#Append
df.write.format('csv').mode('append').save('/Volumes/workspace/zahidschema/unitycatalogbyzahid/Data.csv')

# COMMAND ----------

#overwrite
df.write.format('csv').mode('overwrite').save('/Volumes/workspace/zahidschema/unitycatalogbyzahid/Data.csv')

# COMMAND ----------

#Error
df.write.format('csv').mode('error').save('/Volumes/workspace/zahidschema/unitycatalogbyzahid/Data.csv')

# COMMAND ----------

#ignore
df.write.format('csv').mode('ignore').option('path','/Volumes/workspace/zahidschema/unitycatalogbyzahid/Data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC # format
# MAGIC ### Parquet file format
# MAGIC columner file format<br>
# MAGIC
# MAGIC

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','/Volumes/workspace/zahidschema/unitycatalogbyzahid/Parquetformat/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Table
# MAGIC

# COMMAND ----------

#df.write.format("parquet").saveAsTable("mtableData")
#df.write.format("delta").saveAsTable("your_table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Managed vs External Tables

# COMMAND ----------

# MAGIC %md
# MAGIC # ##spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### TEMP VIEW

# COMMAND ----------

# MAGIC %md 
# MAGIC #Create Temp View()

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'Low Fat'

# COMMAND ----------

# MAGIC %md 
# MAGIC ### to write (save) need to convert to dataframe

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Low Fat'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

