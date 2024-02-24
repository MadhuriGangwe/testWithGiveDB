import sqlite3
import pandas as pd
conn = sqlite3.connect('Data Engineer_ETL Assignment.db')

print ("Opened database successfully");

queryData = conn.execute('''SELECT cu.customer_id as Customer, cu.age as Age, it.item_name as Item, SUM(od.quantity) as Quantity  FROM customers as cu
             JOIN sales as sa ON sa.customer_id=cu.customer_id
             JOIN orders as od ON od.sales_id =sa.sales_id
             JOIN  items AS it ON it.item_id=od.item_id
             WHERE cu.age BETWEEN 18 AND 35 AND od.quantity > 0
             GROUP BY od.item_id, sa.customer_id
             ''')
# Output of the sqlQuery
for data in queryData:
    print(data)
  
# printing results to csv using pandas
sqlQueryForPandas = "SELECT cu.customer_id as Customer, cu.age as Age, it.item_name as Item, SUM(od.quantity) as Quantity FROM customers as cu JOIN sales as sa ON sa.customer_id=cu.customer_id JOIN orders as od ON od.sales_id =sa.sales_id JOIN  items AS it ON it.item_id=od.item_id WHERE cu.age BETWEEN 18 AND 35 AND od.quantity > 0 GROUP BY od.item_id, sa.customer_id"
df = pd.read_sql_query(sqlQueryForPandas, conn)
df.to_csv('queryData.csv', sep=';' ,index=False)
    