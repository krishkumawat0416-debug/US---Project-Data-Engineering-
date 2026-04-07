import boto3
import os
import pandas as pd   # 👈 add kiya

def check_create_bucket(name):  
    s3 = boto3.client('s3')  

    buckets = s3.list_buckets()  

    for bucket in buckets['Buckets']:
        if bucket['Name'] == name:
            print("Bucket Exists")
            return
     
    s3.create_bucket(
        Bucket=name,   
        CreateBucketConfiguration={
            'LocationConstraint': 'ap-south-1'
        }
    )
    print("Bucket Created")


def upload_files(bucket_name):    

    s3 = boto3.client('s3')

    files = os.listdir("download_files")   

    for file in files:

        if file.endswith(".csv"):   # 👈 sirf CSV file

            file_path = "download_files/" + file  

            try:
                # 🔥 data se year/month nikala
                df = pd.read_csv(file_path, nrows=1)

                year = str(df['Year'][0])
                month = str(df['Month'][0]).zfill(2)

            except:
                print("Error reading:", file)
                continue

            key = year + "/" + month + "/" + file

            s3.upload_file(file_path, bucket_name, key)

            print("File uploaded:", file, "→", key)


# input liya
bucket_name = input("Enter bucket name: ").lower()

check_create_bucket(bucket_name)

upload_files(bucket_name)