'''ECOMMERCE DATABASE FEED'''
import pandas as pd
import pyodbc

conexaoDB=pyodbc.connect(
    DRIVER='ODBC Driver 17 for SQL Server',
    SERVER='EMERSONPC',
    DATABASE='DataScience',
    Trusted_Connection='yes') #Se desejar fazer a conexão com usuário e senha, basta adicionar os seguintes parâmetros: 'usuario=Emerson;' e 'senha=1234;'
cursor=conexaoDB.cursor()


#DATA FEED FROM THE Order_Payments TABLE
df_o_p=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_order_payments_dataset.csv')
df_o_p=df_o_p[df_o_p['order_id'].notna()].reset_index(drop=True)
df_o_p=df_o_p.drop_duplicates(subset='order_id', keep='first').reset_index(drop=True)
df_o_p=df_o_p.apply(lambda x: x.astype(str))


data=[(row.order_id, row.payment_sequential, row.payment_type, row.payment_installments,row.payment_value) for index, row in df_o_p.iterrows()]
table_name='Order_Payments'
query=f"INSERT INTO {table_name} (order_id, payment_sequential, payment_type, payment_installments,payment_value) VALUES (?,?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()


#Geolocation TABLE DATA FEED
df_g=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_geolocation_dataset.csv')
df_g=df_g[df_g['geolocation_zip_code_prefix'].notna()].reset_index(drop=True)
df_g=df_g.drop_duplicates(subset='geolocation_zip_code_prefix', keep='first').reset_index(drop=True)
df_g=df_g.apply(lambda x: x.astype(str))


data=[(row.geolocation_zip_code_prefix, row.geolocation_lat, row.geolocation_lng, row.geolocation_city,row.geolocation_state) for index, row in df_g.iterrows()]
table_name='Geolocation'
query=f"INSERT INTO {table_name} (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city,geolocation_state) VALUES (?,?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()


#Sellers TABLE DATA FEED
df_s=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_sellers_dataset.csv')
df_s=df_s[df_s['seller_id'].notna()].reset_index(drop=True)
df_s=df_s.drop_duplicates(subset='seller_id', keep='first').reset_index(drop=True)
df_s=df_s.apply(lambda x: x.astype(str))

#ELIMINATING LINES THAT DO NOT MATCH THE FOREIGN KEY seller_zip_code_prefix in the PRIMARY KEY geolocation_zip_code_prefix
df_s=df_s.apply(lambda x: x if x['seller_zip_code_prefix'] in df_g['geolocation_zip_code_prefix'].values else None, axis=1)
df_s=df_s[df_s['seller_zip_code_prefix'].notna()].reset_index(drop=True)


data=[(row.seller_id, row.seller_zip_code_prefix, row.seller_city, row.seller_state) for index, row in df_s.iterrows()]
table_name='Sellers'
query=f"INSERT INTO {table_name} (seller_id, seller_zip_code_prefix, seller_city, seller_state) VALUES (?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()


#PRODUCTS TABLE DATA FEED
df_p=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_products_dataset.csv')
df_p=df_p[df_p['product_id'].notna()].reset_index(drop=True)
df_p=df_p.drop_duplicates(subset='product_id', keep='first').reset_index(drop=True)
df_p=df_p.apply(lambda x: x.astype(str))


data=[(row.product_id, row.product_category_name, row.product_name_lenght, row.product_description_lenght,row.product_photos_qty, row.product_weight_g, row.product_length_cm, row.product_height_cm, row.product_width_cm) for index, row in df_p.iterrows()]
table_name='Products'
query=f"INSERT INTO {table_name} (product_id, product_category_name, product_name_lenght, product_description_lenght,product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm) VALUES (?,?,?,?,?,?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()


#DATA FEED FROM THE Order_Items TABLE
df_o_i=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_order_items_dataset.csv')
df_o_i=df_o_i[df_o_i['order_id'].notna()].reset_index(drop=True)
df_o_i=df_o_i.drop_duplicates(subset='order_id', keep='first').reset_index(drop=True)
df_o_i=df_o_i.apply(lambda x: x.astype(str))

#ELIMINATING LINES THAT DO NOT MATCH FOREIGN KEY seller_id in PRIMARY KEY product_id
df_o_i=df_o_i.apply(lambda x: x if x['product_id'] in df_p['product_id'].values else None, axis=1)
df_o_i=df_o_i[df_o_i['product_id'].notna()].reset_index(drop=True)

#ELIMINATING LINES THAT DO NOT MATCH THE FOREIGN KEY seller_id in the PRIMARY KEY seller_id
df_o_i=df_o_i.apply(lambda x: x if x['seller_id'] in df_s['seller_id'].values else None, axis=1)
df_o_i=df_o_i[df_o_i['seller_id'].notna()].reset_index(drop=True)


data=[(row.order_id, row.order_item_id, row.product_id, row.seller_id, row.shipping_limit_date, row.price, row.freight_value) for index, row in df_o_i.iterrows()]
table_name='Order_Items'
query=f"INSERT INTO {table_name} (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) VALUES (?,?,?,?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()


#DATA FEED FROM THE Customers TABLE
df_c=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_customers_dataset.csv')
df_c=df_c.drop_duplicates(subset='customer_id', keep='first').reset_index(drop=True)
df_c=df_c[df_c['customer_id'].notna()].reset_index(drop=True)
df_c=df_c.apply(lambda x: x.astype(str))

#ELIMINATING LINES THAT DO NOT MATCH THE FOREIGN KEY customer_zip_code_prefix in the PRIMARY KEY geolocation_zip_code_prefix
df_c=df_c.apply(lambda x: x if x['customer_zip_code_prefix'] in df_g['geolocation_zip_code_prefix'].values else None, axis=1)
df_c=df_c[df_c['customer_zip_code_prefix'].notna()].reset_index(drop=True)

data=[(row.customer_id, row.customer_unique_id, row.customer_zip_code_prefix, row.customer_city, row.customer_state) for index, row in df_c.iterrows()]
table_name='Customers'
query=f"INSERT INTO {table_name} (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES (?,?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()


#DATA FEED FROM THE Order_Reviews TABLE
df_o_r=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_order_reviews_dataset.csv')
df_o_r=df_o_r.drop_duplicates(subset='order_id', keep='first').reset_index(drop=True)
df_o_r=df_o_r[df_o_r['order_id'].notna()].reset_index(drop=True)
df_o_r=df_o_r.apply(lambda x: x.astype(str))


data=[(row.review_id, row.order_id, row.review_score, row.review_comment_title,row.review_comment_message, row.review_creation_date, row.review_answer_timestamp) for index, row in df_o_r.iterrows()]
table_name='Order_Reviews'
query=f"INSERT INTO {table_name} (review_id, order_id, review_score, review_comment_title,review_comment_message, review_creation_date, review_answer_timestamp) VALUES (?,?,?,?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()



#DATA FEED FROM THE ORDER TABLE
df_o=pd.read_csv(r'E:\Data Science\Datasets\Varejo\olist_orders_dataset.csv')
df_o=df_o.drop_duplicates(subset='customer_id', keep='first').reset_index(drop=True)
df_o=df_o[df_o['customer_id'].notna()].reset_index(drop=True)
df_o=df_o.apply(lambda x: x.astype(str))

#ELIMINATING LINES THAT DO NOT MATCH THE FOREIGN KEY order id in the PRIMARY KEY order_id
df_o=df_o.apply(lambda x: x if x['order_id'] in df_o_r['order_id'].values else None, axis=1)
df_o=df_o[df_o['order_id'].notna()].reset_index(drop=True)

#ELIMINATING LINES THAT DO NOT MATCH THE FOREIGN KEY order id in the PRIMARY KEY order_id
df_o=df_o.apply(lambda x: x if x['order_id'] in df_o_p['order_id'].values else None, axis=1)
df_o=df_o[df_o['order_id'].notna()].reset_index(drop=True)

#ELIMINATING LINES THAT DO NOT MATCH THE FOREIGN KEY order id in the PRIMARY KEY order_id
df_o=df_o.apply(lambda x: x if x['order_id'] in df_o_i['order_id'].values else None, axis=1)
df_o=df_o[df_o['order_id'].notna()].reset_index(drop=True)

#ELIMINATING LINES THAT DO NOT MATCH THE FOREIGN KEY customer_id in the PRIMARY KEY customer_id
df_o=df_o.apply(lambda x: x if x['customer_id'] in df_c['customer_id'].values else None,axis=1)
df_o=df_o[df_o['customer_id'].notna()].reset_index(drop=True)


data=[(row.order_id, row.customer_id, row.order_status, row.order_purchase_timestamp,row.order_approved_at,row.order_delivered_carrier_date, row.order_delivered_customer_date,row.order_estimated_delivery_date) for index, row in df_o.iterrows()]
table_name='Orders'
query=f"INSERT INTO {table_name} (order_id, customer_id, order_status, order_purchase_timestamp,order_approved_at,order_delivered_carrier_date, order_delivered_customer_date,order_estimated_delivery_date) VALUES (?,?,?,?,?,?,?,?)"
cursor.execute(f"DELETE FROM {table_name}")
cursor.executemany(query, data)
cursor.commit()
cursor.close()
conexaoDB.close()






