import json
import boto3 # library used to access AWS API
import os
import csv
import psycopg2
import psycopg2.extras
from connect_db import *

def lambda_handler(event, context):
    # TODO implement
    ssm_name = 'mugshot_cafe_redshift_settings'
    os.chdir('/tmp')
    
    s3 = boto3.resource('s3')
    filename = event["Records"][0]["s3"]["object"]["key"] 
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3.Bucket(bucket).download_file(filename,"data.csv")
    print(filename)
    #s3.Bucket("mugshotbucket").download_file("london_piccadilly_03-07-2024_09-00-00.csv",'data.csv')
    #Loading data from data source and printing
    #filename = 'data.csv'
    keys = ('Date and time', 'Location','Name', 'Order', 'Total', 'Payment Type', 'Card Number')
    with open ("data.csv", 'r') as data:
        reader = csv.DictReader(data,keys)
        mugshot = list()
        for row in reader:
            mugshot.append(row)
    
    ###Transform stage - defining functions###
    def remove_sens_data(input_data_list : list,sens_data_keys:list):
        for dicts in input_data_list:
            for key in sens_data_keys:
                if key in dicts:
                    del dicts[key]
    
    def split_date_time(input_data_list : list):
        for dicts in input_data_list:
            dt_temp = dicts["Date and time"]
            date = dt_temp[0:10]
            time = dt_temp[11:16]
            dicts["Date"] = date
            dicts["Time"] = time
            del dicts["Date and time"]
    
    def split_order(input_data_list : list):
        for dicts in input_data_list:
            product_dupe_tag = 0
            order_dicts_list=[]
            split_order_list = dicts["Order"].split(", ")
            for orders in split_order_list:
                product_dupe_tag = 0
                #order_list.append(orders.split(" - "))
                templist = orders.split(" - ")
                if len(templist)>2:
                    price = templist[len(templist)-1]
                    name = ""
                    for items in templist:  
                        if items != price:
                            name += items
                        name +=" "
                        name.rstrip()
                else:
                    name = templist[0]
                    price = templist[1]
                for products in order_dicts_list:
                    if products["Name"] == name:
                        products["Quantity"] = products["Quantity"] + 1
                        product_dupe_tag=1
                if product_dupe_tag !=1:
                    quantity = 1
                    order_dicts_list.append({"Name":name,"Price":price,"Quantity":quantity})
                    product_dupe_tag = 0
                    
    
            dicts["Order_dict"] = order_dicts_list
            del dicts["Order"]
    def insert_data_into_db(connection,cursor, transactions_data):
    #try:
        
            print("insert started")
            transaction_values = []
            product_values = []
            unique_prods=[]
            insert_prods=[]
            order_items_list = []
            insert_order_item_sql = """
                    INSERT INTO order_items (transaction_id, product_id, product_quantity)
                    VALUES (%s, %s, %s)
                    """
            select_product_sql = "SELECT * FROM products"
            insert_transaction_sql = """
                INSERT INTO transactions (date, time, city, total_cost, payment_method)
                VALUES (%s, %s, %s, %s, %s)
                """
            insert_products_sql ="""
                INSERT INTO products (product_name,product_price)
                VALUES (%s, %s)
                """
                
            
            
            select_transaction_sql = "select transaction_id,time FROM transactions where date = %s and city = %s"
            for transaction in transactions_data:
                # insert into transactions table
                transaction_values.append((transaction['Date'],
                    transaction['Time'],
                    transaction['Location'],
                    transaction['Total'],
                    transaction['Payment Type']))
            print("starting execute")
            with open ("/tmp/transactions_data.csv","w",newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(["date","time","city","total_cost","payment_method"]) # write headers
                csv_writer.writerows(transaction_values)
            s3 = boto3.resource('s3')
            bucket = s3.Bucket('mugshotbucketoutput')
            key = 'lambda_outputs/transaction_'+ filename
            bucket.upload_file('/tmp/transactions_data.csv', key)
            print('file uploaded')
            #load
            copy_sql = "COPY transactions (date, time, city, total_cost, payment_method) FROM %s iam_role 'arn:aws:iam::992382716453:role/RedshiftS3Role' delimiter ',' IGNOREHEADER 1"
            bucket_filename ="s3://mugshotbucketoutput/" + key
            cursor.execute(copy_sql,(bucket_filename,))
            #
            print("transactions complete")
            cursor.execute(select_transaction_sql, (transactions_data[0]["Date"],transactions_data[0]['Location'],))
            transaction_id_list = cursor.fetchall()
            for transaction in transactions_data:
                for items in transaction['Order_dict']:
                    product_values.append((items['Name'],items['Price'],items['Quantity']))
                    unique_prods.append((items['Name'],items['Price']))
            unique_prods = [t for t in (set(tuple(i) for i in unique_prods))]
            cursor.execute(select_product_sql)
            product_id_list = cursor.fetchall()
            if len(product_id_list) >0:
                for x in unique_prods:
                    found_flag=0
                    for y in product_id_list:
                        if x[0] == y[1]:
                            found_flag = 1
                    if found_flag == 0:
                        insert_prods.append(x)
            else:
                insert_prods = unique_prods
            if len(insert_prods) > 0:
                a=1
                with open ("/tmp/products_data.csv","w",newline='') as f:
                    csv_writer = csv.writer(f)
                    csv_writer.writerow(["product_id","product_name","product_price"]) # write headers
                    csv_writer.writerows(insert_prods)
                s3 = boto3.resource('s3')
                bucket = s3.Bucket('mugshotbucketoutput')
                key = 'lambda_outputs/products_' + filename
                bucket.upload_file('/tmp/products_data.csv', key)
                print("csv output")
                ## ABOVE THIS INTO ET 
                #load
                copy_sql = "COPY products (product_name,product_price) FROM %s iam_role 'arn:aws:iam::992382716453:role/RedshiftS3Role' delimiter ',' IGNOREHEADER 1"
                bucket_filename ="s3://mugshotbucketoutput/" + key
                cursor.execute(copy_sql,(bucket_filename,))
                #
                
                
            cursor.execute(select_product_sql)
            print("products complete")
            product_id_list = cursor.fetchall()
            for transaction in transactions_data:
                for orders in transaction['Order_dict']:
                    for items in transaction_id_list:
                        if transaction['Time'] == items[1]:
                            transaction_id = items[0]
                    for items in product_id_list:
                        if orders["Name"] == items[1]:
                            product_id = items[0]
                    product_quantity = orders["Quantity"]
                    order_items_list.append((transaction_id,product_id,product_quantity))
        
            
        
            print("order items complete")
            with open ("/tmp/order_items_data.csv","w",newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(["transaction_id","product_id","product_quantity_"]) # write headers
                csv_writer.writerows(order_items_list)
            s3 = boto3.resource('s3')
            bucket = s3.Bucket('mugshotbucketoutput')
            key = 'lambda_outputs/order_items_' + filename
            bucket.upload_file('/tmp/order_items_data.csv', key)
            print("csv output")
            #load
            copy_sql = "COPY order_items (transaction_id,product_id,product_quantity) FROM %s iam_role 'arn:aws:iam::992382716453:role/RedshiftS3Role' delimiter ',' IGNOREHEADER 1"
            bucket_filename ="s3://mugshotbucketoutput/" + key
            cursor.execute(copy_sql,(bucket_filename,))
            #

            print("file created")
        
    
        
    remove_sens_data(mugshot,['Name','Card Number'])
    split_date_time(mugshot)
    split_order(mugshot)
    redshift_details = get_ssm_param(ssm_name)
    connection, cursor = open_sql_database_connection_and_cursor(redshift_details)
    insert_data_into_db(connection,cursor,mugshot)
    connection.commit()
    print("connection complete")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        'mugshot': mugshot[0]
    }
