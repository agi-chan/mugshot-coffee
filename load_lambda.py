import json
import boto3
import os
import psycopg2
from connect_db import *
import pandas as pd

def lambda_handler(event, context):
    # Create an SQS client
    sqs = boto3.client('sqs', region_name='eu-west-1')  # Specify the region
    os.chdir('/tmp')
    
    queue_url = 'https://sqs.eu-west-1.amazonaws.com/992382716453/mugshot-test'
    
    # Receive messages from SQS
    #response = sqs.receive_message(
    #    QueueUrl=queue_url,
    #    MaxNumberOfMessages=1,
    #    VisibilityTimeout=10,
    #    WaitTimeSeconds=0
    #)
#
    #messages = response.get('Messages', [])
    message = json.loads(event["Records"][0]["body"])
    transactions = message[0]
    transactions_df = pd.DataFrame(transactions)
    products = message[1]
    full_data = message[2]
    filename = message[3]
    #if len(messages) == 0:
    #    print("No messages in queue.")
    #    return {
    #        'statusCode': 200,
    #        'body': json.dumps('No messages in queue.')
    #    }
    
    #error_messages = []
    #print(messages)
    #for message in messages:
       # try:
    # Process each message
    #message_body = json.loads(message['Body'])  # Assuming message body is JSON
    #filename = message_body['filename']
    #print(message_body[0])
    
    #establish connection
    ssm_name = 'mugshot_cafe_redshift_settings'
    redshift_details = get_ssm_param(ssm_name)
    connection, cursor = open_sql_database_connection_and_cursor(redshift_details)
    
    # Load data into Redshift
    load_data_to_redshift(connection,cursor,transactions_df,products,full_data,filename)
    
    # Delete the message from the queue once processed
    #receipt_handle = message['ReceiptHandle']
    #sqs.delete_message(
    #    QueueUrl=queue_url,
    #    ReceiptHandle=receipt_handle
    #)
    
    print(f"Processed message for filename: {filename}")
        
        # except KeyError as e:
        #     error_messages.append(f"KeyError: {str(e)}")
        #     continue
        
        # except Exception as e:
        #     error_messages.append(f"Error processing message: {str(e)}")
        #     continue
    
    # After processing all messages, check if there were any errors
    #if error_messages:
    #    # If there were errors, return 500 status code with error messages
    #    return {
    #        'statusCode': 500,
    #        'body': json.dumps(error_messages)
    #    }
    #else:
    #    # If no errors, return 200 status code with success message
    return {
        'statusCode': 200,
        'body': json.dumps('Messages processed successfully.')
    }

def load_data_to_redshift(conn,cursor,transactions,products,fulldata,filename):
#     # Redshift connection details
#     redshift_endpoint = 'your-redshift-endpoint.amazonaws.com'
#     redshift_port = '5439'
#     redshift_user = 'your-redshift-username'
#     redshift_password = 'your-redshift-password'
#     redshift_dbname = 'your-redshift-database'
    
#     # Connect to Redshift
#     conn = psycopg2.connect(
#         dbname=redshift_dbname,
#         user=redshift_user,
#         password=redshift_password,
#         host=redshift_endpoint,
#         port=redshift_port
#     )
    
    # cursor = conn.cursor()
    
    try:
        
        # Example: Load transactions data
        copy_sql = """
            COPY transactions (date, time, city, total_cost, payment_method)
            FROM %s
            IAM_ROLE 'arn:aws:iam::992382716453:role/RedshiftS3Role'
            CSV
            DELIMITER ','
            IGNOREHEADER 1
        """
        #bucket_filename = f's3://mugshotbucketoutput/lambda_outputs/transaction_{filename}'
        #cursor.execute(copy_sql, (bucket_filename,))
        
        # Example: Load products data
        copy_sql_products = """
            COPY products (product_name, product_price)
            FROM %s
            IAM_ROLE 'arn:aws:iam::992382716453:role/RedshiftS3Role'
            CSV
            DELIMITER ','
            IGNOREHEADER 1
        """
        #bucket_filename_products = f's3://mugshotbucketoutput/lambda_outputs/products_{filename}'
        #cursor.execute(copy_sql_products, (bucket_filename_products,))
        
        # Example: Load order items data
        copy_sql_order_items = """
            COPY order_items (transaction_id, product_id, product_quantity)
            FROM %s
            IAM_ROLE 'arn:aws:iam::992382716453:role/RedshiftS3Role'
            CSV
            DELIMITER ','
            IGNOREHEADER 1
        """
        insert_prods=[]
        order_items_list = []
        transactions.to_csv("transactions.csv",index = False)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('mugshotbucketoutput')
        key = 'lambda_outputs/transactions_'+ filename
        bucket.upload_file('/tmp/transactions.csv', key)
        bucket_filename ="s3://mugshotbucketoutput/" + key
        cursor.execute(copy_sql,(bucket_filename,))
        print("transactions complete")
        
        
        
        
        select_transaction_sql = "select transaction_id,time FROM transactions where date = %s and city = %s"
        cursor.execute(select_transaction_sql, (fulldata[0]["Date"],fulldata[0]['Location'],))
        transaction_id_list = cursor.fetchall()
        
        #cursor.execute(select_product_sql)
        #product_id_list = cursor.fetchall()
        
        
        select_product_sql = "SELECT * FROM products"
        cursor.execute(select_product_sql)
        product_id_list = cursor.fetchall()
        if len(product_id_list) >0:
            for x in products:
                found_flag=0
                for y in product_id_list:
                    if x[0] == y[1]:
                        found_flag = 1
                if found_flag == 0:
                    insert_prods.append(x)
        else:
            insert_prods = products
        if len(insert_prods) > 0:
            products_df = pd.DataFrame(insert_prods)
            products_df.columns = ["product_name","product_price"]
            products_df.to_csv("products.csv",index = False)
            bucket = s3.Bucket('mugshotbucketoutput')
            key = 'lambda_outputs/products_' + filename
            bucket.upload_file('/tmp/products.csv', key)
            bucket_filename ="s3://mugshotbucketoutput/" + key
            cursor.execute(copy_sql_products,(bucket_filename,))
            #cursor.execute(copy_sql_products,("/tmp/products.csv",))
         
        print("products complete")   
        cursor.execute(select_product_sql)
        product_id_list = cursor.fetchall()
        for transaction in fulldata:
            for orders in transaction['Order_dict']:
                for items in transaction_id_list:
                    if transaction['Time'] == items[1]:
                        transaction_id = items[0]
                for items in product_id_list:
                    if orders["Name"] == items[1]:
                        product_id = items[0]
                product_quantity = orders["Quantity"]
                order_items_list.append((transaction_id,product_id,product_quantity))
        order_items_df = pd.DataFrame(order_items_list)
        order_items_df.columns = ["transaction_id","product_id","product_quantity"]
        order_items_df.to_csv("order_items_df.csv",index = False)
        bucket = s3.Bucket('mugshotbucketoutput')
        key = 'lambda_outputs/order_items_' + filename
        bucket.upload_file('/tmp/order_items_df.csv', key)
        bucket_filename ="s3://mugshotbucketoutput/" + key
        cursor.execute(copy_sql_order_items,(bucket_filename,))
        #cursor.execute(copy_sql_order_items,("/tmp/order_items_df.csv",))
                
        
        
        
        
        
        #bucket_filename_order_items = f's3://mugshotbucketoutput/lambda_outputs/order_items_{filename}'
        #cursor.execute(copy_sql_order_items, (bucket_filename_order_items,))
        
        #conn.commit()
        print(f"Data loaded successfully for filename: {filename}")
    
    except Exception as e:
        conn.rollback()
        print(f"Error loading data to Redshift: {str(e)}")
        raise
    
    finally:
        cursor.close()
        conn.close()