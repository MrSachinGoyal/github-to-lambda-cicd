import json
import pandas as pd
import boto3

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

sns_arn = "arn:aws:sns:ap-southeast-2:211125395305:processed-data-notification"

def lambda_handler(event, context):
    # TODO implement
    print(event)

    try:
        # getting bucket and key details from event parameter
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_key)

        response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        print(response['Body'])

        # reading S3 data into pandas dataframe
        orders_df = pd.read_csv(response['Body'], sep=",")
        print(orders_df.head())

        # filter the delivered orders
        delivered_orders = orders_df[orders_df['status'] == 'delivered']

        # sent the processed data to the destination bucket 
        destination_bucket = 'doordash-processed-data'
        json_data = delivered_orders.to_json(orient='records')
        file_name = "2024-03-26_processed-data.json"

        s3_client.put_object(Bucket=destination_bucket, key=file_name, Body=json_data)
        print(f"File '{file_name}' uploaded to bucket '{destination_bucket}'")

        message = "Input S3 File {} has been processed succesfuly !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')

    return {
        'statusCode': 200,
        'body': json.dumps('Job Completed Successfully')
    }