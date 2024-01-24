# Databricks notebook source
filename=dbutils.widgets.get('filename')
fnamewithoutExt=filename.split('.')[0]
filename="orders_neww.csv"
print(filename)

# COMMAND ----------

dbServer='asrithasqlserver'
dbPort='1433'
dbName='asrithasqldb'
dbUser='asritha'
dbPassword='sql-pwd'
databricksScope='salesprojectScope'

# COMMAND ----------

storageAccountKey=dbutils.secrets.get(scope=databricksScope,key='storage-account-key')

# COMMAND ----------

alreadyMounted=False

for x in dbutils.fs.mounts():
    if x.mountPoint=="/mnt/sales":
        alreadyMounted=True
        break
print(alreadyMounted)

# COMMAND ----------

if not alreadyMounted:
    dbutils.fs.mount(
      source="wasbs://sales@asrithasa.blob.core.windows.net",
      mount_point="/mnt/sales",
      extra_configs={'fs.azure.account.key.asrithasa.blob.core.windows.net':storageAccountKey })
    
    alreadyMounted=True
    print("mounting done successfully")
else:
    print("already mounted")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sales/landing
# MAGIC

# COMMAND ----------

spark

# COMMAND ----------

orders_df=spark.read.csv('/mnt/sales/landing/{}'.format(filename),inferSchema=True,header=True)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

errorFlg=False
orders_count=orders_df.count()
print(orders_count)

distinct_orders_count=orders_df.select('order_id').distinct().count()
print(distinct_orders_count)

if orders_count!=distinct_orders_count:
    errorFlg=True

if errorFlg:
    dbutils.fs.mv('/mnt/sales/landing/{}'.format(filename),'/mnt/sales/discarded')
    dbutils.notebook.exit('{"errorFlg":"true","errorMsg":"Orderid is repeated"}')
orders_df.createOrReplaceTempView('orders')


# COMMAND ----------



# COMMAND ----------



dbutils.secrets.listScopes()


# COMMAND ----------

connectionUrl='jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(dbServer,dbPort,dbName,dbUser)
dbPassword=dbutils.secrets.get(scope=databricksScope,key='sql-pwd')
connectionProperties={
    'password':dbPassword,
    'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}

# COMMAND ----------

validStatusDf=spark.read.jdbc(url=connectionUrl,table='dbo.valid_order_status',properties=connectionProperties)

# COMMAND ----------

validStatusDf.createOrReplaceTempView("valid_status")

# COMMAND ----------

invalidRowsDf=spark.sql("select * from orders where order_status not in(select * from valid_status)")

# COMMAND ----------

display(invalidRowsDf)

# COMMAND ----------

if invalidRowsDf.count() >0:
    errorFlg=True

if errorFlg:
    dbutils.fs.mv("/mnt/sales/landing/{}".format(filename),'/mnt/sales/discarded')
    dbutils.notebook.exit('{"errorFlg":"true","errorMsg":"Invalid order status found"}')

else:
    dbutils.fs.mv("/mnt/sales/landing/{}".format(filename),'/mnt/sales/staging')
    

# COMMAND ----------

orderItemsDf=spark.read.csv('/mnt/sales/order_items/order_items.csv',inferSchema=True,header=True)
display(orderItemsDf)
orderItemsDf.createOrReplaceTempView("order_items")

# COMMAND ----------

customerDf=spark.read.jdbc(url=connectionUrl,table='dbo.customers',properties=connectionProperties)
display(customerDf)
customerDf.createOrReplaceTempView("customers")

# COMMAND ----------

orders_df=spark.read.csv('/mnt/sales/staging/{}'.format(filename),inferSchema=True,header=True)

# COMMAND ----------

orders_df.createOrReplaceTempView('orders')

# COMMAND ----------

result_df=spark.sql("""
  SELECT
    customers.customer_id,
    customers.customer_fname,
    customers.customer_lname,
    customers.customer_city,
    customers.customer_state,
    customers.customer_zipcode,
    COUNT(orders.order_id) AS num_orders_placed,
    SUM(order_items.order_item_subtotal) AS total_amount
  FROM
    orders
    INNER JOIN customers ON orders.customer_id = customers.customer_id
    INNER JOIN order_items ON orders.order_id = order_items.order_item_order_id
  GROUP BY
    customers.customer_id,
    customers.customer_fname,
    customers.customer_lname,
    customers.customer_city,
    customers.customer_state,
    customers.customer_zipcode
  ORDER BY
    total_amount DESC
""")


# COMMAND ----------

display(result_df)
result_df.write.jdbc(url=connectionUrl,table='dbo.sales_reporting',properties=connectionProperties,mode='overwrite')
