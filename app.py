from flask import Flask, request, jsonify, Response
import boto3
import time
from pymongo import MongoClient
import json
from datetime import datetime
from flask import send_file
from properties import *
from io import BytesIO
from functools import wraps
import threading
import urllib.parse
from bson import ObjectId
from queue import Queue

app = Flask(__name__)


# AWS s3 configuration

AWS_ACCESS_KEY = 'enter your aws access key'
AWS_SECRET_KEY = 'enter you secret key'
S3_BUCKET = 'enter your desired bucket name or already created bucket name'
SNS_TOPIC_ARN = 'create an sns topic and copy paste the ARN here'
textract = boto3.client('textract', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='us-east-1')

client = MongoClient(MONGODB_CONNECTION_STRING)
db = client[MONGODB_DATABASE_NAME]
collection = db[MONGODB_COLLECTION_NAME]

#connecting to s3 programatically through boto3 

s3 = boto3.client('s3',aws_access_key_id = AWS_ACCESS_KEY,
aws_secret_access_key = AWS_SECRET_KEY,region_name = 'us-east-1')

# Authenticate user
def authenticate(username, password):
    user = user_collection.find_one({'username': username})
    if user and user['password'] == password:
        return True
    return False

# Decorator for authentication
def requires_auth(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not authenticate(auth.username, auth.password):
            return Response('Could not verify your credentials. Please provide valid username and password.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
        return func(*args, **kwargs)
    return decorated


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


@app.route('/retrieve',methods=['GET'])
def get_details():
    name = request.args.get('name')
    if name:
        documents = collection.find({'name':name})
        response = []
        for document in documents:
            response.append({
            'name':document['name'],
            'cell':document['cell'],
            'file_name':document['file_name'],
            'time_stamp':document['timestamp'],
            'extracted_text':document['extractedtext']
            })
        return json.dumps(response)
    return 'Invalid request. please provide a valid input name'
    

@app.route('/extracted_text',methods=['GET'])
def get_extracted_text():
    name = request.args.get('name')
    file_name = request.args.get('file')

    if name and file_name:
        document = collection.find_one({'name':name,'file_name':file_name})

        if document:
            extracted_text = document.get('extractedtext')
            return f'Extracted Text for {file_name}:\n{extracted_text}'

        return f'No extracted text available for {file_name}'

    return 'Invalid request. Please provide valid name and file parameters.'


@app.route('/download', methods=['GET'])
def download_file():
    name = request.args.get('file_name')
    if name:
        document = collection.find_one({'file_name': name})
        if document:
            file_name = document['file_name']
            # Get the file from S3 bucket
            file = s3.get_object(Bucket=S3_BUCKET, Key=file_name)
            # Extract the file content
            file_content = file['Body'].read()

            # Set the appropriate headers for binary download
            headers = {
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': f'attachment; filename={file_name}'
            }

            # Return the file as a binary response
            return Response(BytesIO(file_content), headers=headers)

        return 'No file found for the given name'

    return 'Invalid request. Please provide a valid name parameter.'


@app.route('/delete', methods=['DELETE'])
@requires_auth
def delete_file():
    file_name = request.args.get('file_name')
    name = request.args.get('name')
    username = request.authorization.username
    password = request.authorization.password

    if file_name:
        # Check if the file exists in the S3 bucket
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=file_name)
        if 'Contents' not in response:
            return jsonify({'message': 'File does not exist.'}), 404

        # Authenticate the user
        if not authenticate(username, password):
            return jsonify({'message': 'Invalid username or password.'}), 401

        # Delete the file from the S3 bucket
        s3.delete_object(Bucket=S3_BUCKET, Key=file_name)

        # Delete the file document from MongoDB
        collection.delete_one({'name': name, 'file_name': file_name})

        return jsonify({'message': 'File deleted successfully.'}), 200

if __name__ == '__main__':
    app.run(debug=True)
