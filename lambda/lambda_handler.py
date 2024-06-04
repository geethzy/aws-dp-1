# def lambda_handler(event, context):
#     print("lambda1 invoked - test3")

import json
import boto3

def lambda_handler(event, context):
    print("lambda1 invoked - cdk")
    glue = boto3.client('glue')
    crawler_name = 'test-lambda-crawler'

    try:
        response = glue.get_crawler(Name=crawler_name)
        crawler_state = response['Crawler']['State']

        if crawler_state != 'RUNNING':

            response = glue.start_crawler(Name=crawler_name)
            print(f"Started Glue crawler: {crawler_name}")
        else:
            print(f"Glue crawler {crawler_name} is already running.")
    except glue.exceptions.EntityNotFoundException:

        print(f"Glue crawler {crawler_name} not found.")

    print("lambda1 finished - cdk")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }