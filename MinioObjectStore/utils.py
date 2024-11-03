from .views import *
import re
import os
from minio import Minio
import paramiko
from scp import SCPClient
from io import BytesIO
import datetime


def CreateSshClient(server, port, user, password):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=server, port=port, username=user, password=password)
        print("Connection established..")
        return client
    except Exception as e:
        print(e)
        return False

# Formtting size to human readable 
def FormatSize(sizeInBytes):
    # Convert bytes to human-readable units
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if sizeInBytes < 1024.0:
            return f"{sizeInBytes:.2f} {unit}"
        sizeInBytes /= 1024.0
    return f"{sizeInBytes:.2f} TB"


def InitializeClient(minioEndPoint, minioAccessKey, minioSecretKey, minioSecure):
    try:
        client = Minio(
            endpoint=minioEndPoint,
            access_key=minioAccessKey,
            secret_key=minioSecretKey,
            secure=minioSecure,
        )
        return client
    except Exception as e:
        print(f"Error initializing MinIO client: {e}")
        return None

def human_readable_size(size):
    if size < 1024:
        return f"{size} B"
    elif size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024 ** 3:
        return f"{size / (1024 ** 2):.2f} MB"
    elif size < 1024 ** 4:
        return f"{size / (1024 ** 3):.2f} GB"
    else:
        return f"{size / (1024 ** 4):.2f} TB"


def ListBuckets(client):
    try:
        buckets = client.list_buckets()
        resp=[]
        total_storage_size = 0
        if buckets:
            for bucket in buckets:
                total_size = 0
                for obj in client.list_objects(bucket.name, recursive=True):
                    total_size += obj.size
                
                total_storage_size += total_size
                    
                resp.append({"name" : bucket.name, 
                             "estimated_size": human_readable_size(total_size)})
            
            total_storage_size_human = human_readable_size(total_storage_size)
            return {
            "buckets": resp,
            "total_storage_size": total_storage_size_human
            }
            # return resp
    except Exception as e:
        print(f"Error checking connection: {e}")
        return False

def ValidateBucketName(name):
    pattern = r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'
    return bool(re.match(pattern, name))


def EnsureBucketExists(client, name):
    if not ValidateBucketName(name):
        print(f"Bucket name '{name}' is invalid.")
        return False
    try:
        if client.bucket_exists(name):
            print(f"Bucket '{name}' exists.")
            return True
        else:
            # If the bucket doesn't exist, create it
            client.make_bucket(name)
            print(f"Bucket '{name}' created.")
            return True
    except Exception as e:
        print(f"Error ensuring bucket '{name}' exists: {e}")
        return False


def DownloadFilesFromBucket(bucketName, downloadDir, localPath, client, isRemote, remoteHost, remoteUser, remotePassword):
    try:
        if not EnsureBucketExists(client, bucketName):
            print(f"Bucket '{bucketName}' does not exist.")
            return False
        objects = client.list_objects(bucketName, recursive=True)
        
        if isRemote:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(remoteHost, username=remoteUser, password=remotePassword)

            with SCPClient(ssh.get_transport()) as scp:
                for obj in objects:
                    minio_obj = client.get_object(bucketName, obj.object_name)
                    data = BytesIO(minio_obj.read())  
                    data.seek(0)  

                    remoteFilePath = os.path.join(downloadDir, obj.object_name)
                    remoteDir = os.path.dirname(remoteFilePath)

                    stdin, stdout, stderr = ssh.exec_command(f'mkdir -p {remoteDir}')
                    stdout.channel.recv_exit_status() 

                    scp.putfo(data, remoteFilePath)
                    print(f"Transferred '{obj.object_name}' to remote '{remoteFilePath}'.")
                    minio_obj.close()

        else:    
            for obj in objects:
                localFilePath = os.path.join(localPath, obj.object_name)
                localDir = os.path.dirname(localFilePath)
                
                if not os.path.exists(localDir):
                    os.makedirs(localDir)
                
                client.fget_object(bucketName, obj.object_name, localFilePath)
                print(f"Downloaded '{obj.object_name}' from bucket '{bucketName}' to '{localFilePath}'.")
            return localPath
        return downloadDir
    except Exception as e:
        print(f"Error downloading files from bucket '{bucketName}': {str(e)}")
        return False

def UploadFiles(client, bucketName, filePath, isRemote, remoteHost, remoteUser, remotePassword):
    try:
        if EnsureBucketExists(client, bucketName):
            if isRemote:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(remoteHost, username=remoteUser, password=remotePassword)

                stdin, stdout, stderr = ssh.exec_command(f"find {filePath} -type f")
                file_paths = stdout.readlines()

                for file_path in file_paths:
                    file_path = file_path.strip()

                    minio_path = os.path.relpath(file_path, filePath)
                    
                    print(f"Uploading remote file '{file_path}' to bucket '{bucketName}' as '{minio_path}'")

                    sftp = ssh.open_sftp()
                    with sftp.open(file_path, 'rb') as remote_file:
                        file_size = sftp.stat(file_path).st_size

                        client.put_object(bucketName, minio_path, remote_file, file_size)

                    print(f"Uploaded '{file_path}' to MinIO bucket '{bucketName}'")
                
                sftp.close()
                ssh.close()
                
            else:
                for root, dirs, files in os.walk(filePath):
                    for file in files:
                        file_path = os.path.join(root, file)
                        minio_path = os.path.relpath(file_path, filePath)

                        print(f"Uploading file '{file_path}' to bucket '{bucketName}' as '{minio_path}'")
                        with open(file_path, 'rb') as data:
                            client.put_object(bucketName, minio_path, data, os.path.getsize(file_path))
            return True
        else:
            print(f"Failed to upload file '{filePath}' to bucket '{bucketName}'.")
            return False
    except Exception as e:
        print(f"Error uploading file '{filePath}' to bucket '{bucketName}': {str(e)}")
        return False


def DownloadAllBucketsToRemote(client, isRemote, remoteHost, remoteUser, remotePassword, remoteBackupPath, localPath):
    try:
        buckets = client.list_buckets()
        
        if isRemote:

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(remoteHost, username=remoteUser, password=remotePassword)
            remoteBackupPath = os.path.join(remoteBackupPath,f'{int(datetime.datetime.now().timestamp())}_Minio_Backup')
            
            # skip_keywords = [
            #         "pcap-pipeline", 
            #         "ip-pipeline", 
            #         "392741d5-fd92-4c05-9dd7-096531263b00", 
            #         "9da92015-b715-477f-9382-ff5bf8654a06"
            #     ]
            with SCPClient(ssh.get_transport()) as scp:
                for bucket in buckets:
                    bucketName = bucket.name
                    # if any(keyword in bucketName for keyword in skip_keywords):
                    #     print(f"Skipping bucket '{bucketName}' as it contains one of the skip keywords.")
                    #     continue
                    print(f"Processing bucket: {bucketName}")
                    
                    try:
                        if not EnsureBucketExists(client, bucketName):
                            print(f"Bucket '{bucketName}' does not exist.")
                            continue
                        
                        objects = client.list_objects(bucketName, recursive=True)

                        for obj in objects:
                            try:
                                minio_obj = client.get_object(bucketName, obj.object_name)
                                data = BytesIO(minio_obj.read())
                                data.seek(0) 
                                
                                remoteFilePath = os.path.join(remoteBackupPath, bucketName, obj.object_name)
                                remoteDir = os.path.dirname(remoteFilePath)
                                ssh.exec_command(f'mkdir -p {remoteDir}')

                                scp.putfo(data, remoteFilePath)
                                print(f"Transferred '{obj.object_name}' to remote '{remoteFilePath}'.")
                                
                                minio_obj.close()
                                
                            except Exception as objError:
                                print(f"Error transferring object '{obj.object_name}': {str(objError)}")
                                continue

                    except Exception as bucketError:
                        print(f"Error processing bucket '{bucketName}': {str(bucketError)}")
                        continue
        else:
            # skip_keywords = [
            #         "pcap-pipeline", 
            #         "ip-pipeline", 
            #         "392741d5-fd92-4c05-9dd7-096531263b00", 
            #         "9da92015-b715-477f-9382-ff5bf8654a06"
            #     ]
            for bucket in buckets:
                bucketName = bucket.name
                # if any(keyword in bucketName for keyword in skip_keywords):
                #     print(f"Skipping bucket '{bucketName}' as it contains one of the skip keywords.")
                #     continue
                # Create a local directory for the bucket
                localBucketDir = os.path.join(localPath, bucketName)

                # Check if the local bucket directory exists
                if not os.path.exists(localBucketDir):
                    os.makedirs(localBucketDir)

                objects = client.list_objects(bucketName, recursive=True)

                for obj in objects:
                    localFilePath = os.path.join(localBucketDir, obj.object_name)
                    localDir = os.path.dirname(localFilePath)

                    # If the file already exists, skip downloading
                    if os.path.exists(localFilePath):
                        print(f"File '{localFilePath}' already exists. Skipping download.")
                        continue

                    # Create local directory for the file if it doesn't exist
                    if not os.path.exists(localDir):
                        os.makedirs(localDir)

                    # Download file to local bucket directory
                    try:
                        print(f"Attempting to download '{obj.object_name}'")
                        client.fget_object(bucketName, obj.object_name, localFilePath)
                        print(f"Downloaded '{obj.object_name}' from bucket '{bucketName}' to '{localFilePath}'.")
                    except Exception as e:
                        print(f"Error downloading '{obj.object_name}': {str(e)}")

    except Exception as e:
        print(f"Error connecting to remote host or processing buckets: {str(e)}")
        return False
    
    finally:
        if isRemote:
            ssh.close()

    return True

def RestoreAllBuucketsFromRemote(client, isRemote, remoteHost, remoteUser, remotePassword, remoteBackupPath, localPath):
    try:
        if isRemote:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(remoteHost, username=remoteUser, password=remotePassword)
            
            stdin, stdout, stderr = ssh.exec_command(f"find {remoteBackupPath} -mindepth 1 -maxdepth 1 -type d")
            bucket_directories = stdout.readlines()
        else:
            bucket_directories = [os.path.join(localPath, d) for d in os.listdir(localPath) if os.path.isdir(os.path.join(localPath, d))]

        for bucket_dir in bucket_directories:
            bucket_dir = bucket_dir.strip()
            bucket_name = os.path.basename(bucket_dir)

            print(f"Restoring bucket: {bucket_name}")

            if EnsureBucketExists(client, bucket_name):
                if isRemote:
                    stdin, stdout, stderr = ssh.exec_command(f"find {bucket_dir} -type f")
                    file_paths = stdout.readlines()

                    for file_path in file_paths:
                        file_path = file_path.strip()
                        minio_path = os.path.relpath(file_path, bucket_dir)

                        print(f"Uploading remote file '{file_path}' to bucket '{bucket_name}' as '{minio_path}'")

                        sftp = ssh.open_sftp()
                        with sftp.open(file_path, 'rb') as remote_file:
                            file_size = sftp.stat(file_path).st_size

                            client.put_object(bucket_name, minio_path, remote_file, file_size)

                        print(f"Uploaded '{file_path}' to MinIO bucket '{bucket_name}'")

                    sftp.close()

                else:
                    for root, dirs, files in os.walk(bucket_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            minio_path = os.path.relpath(file_path, bucket_dir)

                            print(f"Uploading file '{file_path}' to bucket '{bucket_name}' as '{minio_path}'")
                            with open(file_path, 'rb') as data:
                                client.put_object(bucket_name, minio_path, data, os.path.getsize(file_path))

            else:
                print(f"Failed to restore bucket '{bucket_name}'.")

        if isRemote:
            ssh.close()
        return True

    except Exception as e:
        print(f"Error restoring buckets: {str(e)}")
        return False

def CheckRemoteDiskSpace(sshClient, backupPath):
    command = f'df -h {backupPath}'
    stdin, stdout, stderr = sshClient.exec_command(command)

    df_output = stdout.read().decode().strip()
    errorOutput = stderr.read().decode().strip()

    if errorOutput:
        raise Exception(f"Error retrieving available space: {errorOutput}")

    lines = df_output.splitlines()

    if len(lines) < 2:
        raise Exception(f"Invalid output for disk space: {df_output}")

    # e.g., Filesystem      Size  Used Avail Use% Mounted on
    headers = lines[0].split()
    values = lines[1].split()

    # Locate 'Avail' column by index in headers and extract corresponding value from values
    avail_index = headers.index('Avail')
    available_space = values[avail_index]

    # Convert the available space to bytes
    return available_space

def ConvertToBytesB(sizeStr):
    # Convert human-readable sizes (like "500 MB") to bytes
    sizeStr = sizeStr.strip()
    size, unit = float(sizeStr[:-2]), sizeStr[-2:].upper()
    
    if unit == 'KB':
        return size * 1024
    elif unit == 'MB':
        return size * 1024 ** 2
    elif unit == 'GB':
        return size * 1024 ** 3
    elif unit == 'TB':
        return size * 1024 ** 4
    else:
        return size  # Assuming it's already in bytes if no unit

def ConvertToBytes(sizeStr):
    sizeStr = sizeStr.strip().upper()
    
    # Regular expression to match size (e.g., '500M', '10G', etc.)
    size_re = re.match(r'(\d+(\.\d+)?)([KMGT]?)', sizeStr)
    if not size_re:
        raise Exception(f"Invalid size format: {sizeStr}")

    size = float(size_re.group(1))
    unit = size_re.group(3)

    # Map unit to multiplier
    multiplier = {
        'K': 1024,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
        'T': 1024 ** 4
    }.get(unit, 1)  # Default is bytes if no unit

    return int(size * multiplier)
         