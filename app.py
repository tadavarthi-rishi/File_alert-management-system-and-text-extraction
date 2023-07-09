from flask import Flask, request
import boto3
import json
from pymongo import MongoClient



app = Flask(__name__)


# AWS s3 configuration

AWS_ACCESS_KEY = 'enter your aws access key'
AWS_SECRET_KEY = 'enter you secret key'
S3_BUCKET = 'enter your desired bucket name or already created bucket name'
SNS_TOPIC_ARN = 'create an sns topic and copy paste the ARN here'
textract = boto3.client('textract', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='us-east-1')



#connecting to s3 programatically through boto3 

s3 = boto3.client('s3',aws_access_key_id = AWS_ACCESS_KEY,
aws_secret_access_key = AWS_SECRET_KEY,region_name = 'us-east-1')



def get_job_results(job_id):
    response = textract.get_document_text_detection(JobId=job_id)
    status = response['JobStatus']

    while status == 'IN_PROGRESS':
        time.sleep(5)
        response = textract.get_document_text_detection(JobId=job_id)
        status = response['JobStatus']

    if status == 'SUCCEEDED':
        blocks = response['Blocks']
        extracted_text = ''

        for block in blocks:
            if block['BlockType'] == 'LINE':
                extracted_text += block['Text'] + '\n'

        return extracted_text

    return None


def get_sqs_message():
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0
    )

    messages = response.get('Messages', [])
    if messages:
        message = messages[0]
        receipt_handle = message['ReceiptHandle']

        # Delete the message from the queue
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=receipt_handle
        )

        # Extract the message body
        message_body = message['Body']

        # Parse the message body as JSON
        sqs_message = json.loads(message_body)

        # Extract the relevant information from the SQS message
        file_name = sqs_message['Records'][0]['s3']['object']['key']
        print(file_name)

        return message_body

    return None

@app.route('/upload',methods = ['POST'])
def upload_fie():
    # Get the uploaded file from the request
    file = request.files['file']
    # Check if the bucket exists
    bucket_exists = False
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'] == S3_BUCKET:
            bucket_exists = True
            break

        # Create the bucket if it does not exist
        if not bucket_exists:
            s3.create_bucket(Bucket=S3_BUCKET)

        # Upload the file to S3 bucket
        s3.upload_fileobj(file, S3_BUCKET, file.filename)

        # Call Textract to extract text from the document
        response = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': S3_BUCKET, 'Name': file.filename}}
        )

        # Get the job ID for retrieving the extracted text
        job_id = response['JobId']

        # Wait for the Textract job to complete and retrieve the extracted text
        extracted_text = get_job_results(job_id)

        if extracted_text:
            # Store the user's name, mobile number, and file name, extracted text in MongoDB
            timestamp = time.time()
            timestamp_format = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            document = {'name': name, 'cell': cell, 'file_name': file.filename,'timestamp': timestamp_format,'extractedtext':extracted_text}
            collection.insert_one(document)

            return f'File uploaded successfully. Object URL: {object_url}\nExtracted Text:\n{extracted_text}'
        else:
            return f'File uploaded successfully. Object URL: {object_url}\nFailed to extract text.'

    return 'Invalid request. Please provide a file, name, and cell number.'

    return f'File uploaded successfully'


@app.route('/messages', methods=['GET'])
def get_messages():
    message = get_sqs_message()
    if message:
        return f'SQS Message: {message}'
    return 'No messages in SQS'


if __name__ == '__main__':
    app.run(debug=True)
