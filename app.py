from flask import Flask, request
import boto3
import json



app = Flask(__name__)


# AWS s3 configuration

AWS_ACCESS_KEY = 'enter your aws access key'
AWS_SECRET_KEY = 'enter you secret key'
S3_BUCKET = 'enter your desired bucket name or already created bucket name'


#connecting to s3 programatically through boto3 

s3 = boto3.client('s3',aws_access_key_id = AWS_ACCESS_KEY,
aws_secret_access_key = AWS_SECRET_KEY,region_name = 'us-east-1')



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

    return f'File uploaded successfully'


if __name__ == '__main__':
    app.run(debug=True)
