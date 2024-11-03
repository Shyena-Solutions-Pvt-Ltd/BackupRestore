from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import os
from .utils import *


class BucketList(APIView):
    def get(self, request):
        
        minioEndpoint = request.query_params.get('minio_endpoint',None)
        minioAccessKey = request.query_params.get('minio_access_key',None)
        minioSecretKey = request.query_params.get('minio_secret_key',None)
        minioSecure = False
        client = InitializeClient(minioEndpoint, minioAccessKey, minioSecretKey, minioSecure)
        
        if client:
            bucketList = ListBuckets(client)
            if bucketList:
                payload = {
                    "status":True,
                    "message":"List of buckets in object store",
                    "data":bucketList['buckets'],
                    "total_storage_size": bucketList['total_storage_size'],
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status":False,
                    "message":"Error in listing of buckets in object store",
                    "data":None,
                    "error":"Empty bucket cant be listed."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                    "status":False,
                    "message":"Cant able to connect Minio.",
                    "data":None,
                    "error":"Connection Failed."
                }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
    
class MinioBackup(APIView):
    def post(self, request):
        data = request.data

        minioEndpoint = data.get('minio_endpoint',None)
        minioAccessKey = data.get('minio_access_key',None)
        minioSecretKey = data.get('minio_secret_key',None)
        minioSecure = False
        client = InitializeClient(minioEndpoint, minioAccessKey, minioSecretKey, minioSecure)
        
        bucketName = data.get("bucket_name",None)
        backupPath = data.get("backup_path",None)
        localPath = "/tmp/Minio"
        
        isRemote = data.get("remote",False)
        remoteHost = data.get("remote_host",None)
        remoteUser = data.get("remote_user",None)
        remotePort = data.get("remote_port",None)
        remotePassword = data.get("remote_password",None)
        
        if isRemote:
            if not (backupPath and remoteHost and remotePort and remoteUser and remotePassword):
                payload = {
                    "status": False,
                    "message": "Please provide remote credentials with backup path to proceed with backup.",
                    "data": None,
                    "error": "Backup won't proceed without all remote credentials."
                }
                return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                sshclient = CreateSshClient(remoteHost, int(remotePort), remoteUser, remotePassword)
                if sshclient:
                    try:
                        buckets = client.list_buckets()
                        total_storage_size = 0
                        if buckets:
                            for bucket in buckets:
                                total_size = 0
                                for obj in client.list_objects(bucket.name, recursive=True):
                                    total_size += obj.size
                                
                                total_storage_size += total_size
                                
                        remoteSpace = CheckRemoteDiskSpace(sshclient, backupPath)
                        
                        totalSize = ConvertToBytesB(str(total_storage_size))
                        if isinstance(remoteSpace, str):
                            remoteSpace = ConvertToBytes(remoteSpace)
                        
                        if remoteSpace < totalSize:
                            payload = {
                                "status": False,
                                "message": "Not enough space on the remote host for backup.",
                                "required_space": FormatSize(total_storage_size),
                                "available_space": FormatSize(remoteSpace),
                                "error": None
                            }
                            return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)
                    except Exception as e:
                        payload = {
                            "status": False,
                            "message": "Remote backup failed due to an error.",
                            "data": None,
                            "error": str(e)
                        }
                        return Response(payload, status=status.HTTP_400_BAD_REQUEST)
                else:
                    payload = {
                        "status": False,
                        "message": "Remote client connection failed.",
                        "data": None,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_404_NOT_FOUND)
                    
        if bucketName:
            response = DownloadFilesFromBucket(bucketName, backupPath, localPath, client, isRemote, remoteHost, remoteUser, remotePassword)
            if response:
                payload = {
                    "status":True,
                    "message":"Files from the object store are downloaded succesfully.",
                    "data":response,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status":False,
                    "message":"Error in downloading files from bucket.",
                    "data":None,
                    "error":None
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            if DownloadAllBucketsToRemote(client, isRemote, remoteHost, remoteUser, remotePassword, backupPath, localPath):
                return Response({
                    "status":True,
                    "message":"Object Store downloaded succesfully.",
                    "data":backupPath,
                    "error":None
                },status=status.HTTP_200_OK)
            else:
                return Response({
                    "status":False,
                    "message":"Error in downloading files from bucket.",
                    "data":None,
                    "error":None
                },status=status.HTTP_400_BAD_REQUEST)

class MinioRestore(APIView):
    def post(self, request):
        data = request.data
        
        minioEndpoint = data.get('minio_endpoint',None)
        minioAccessKey = data.get('minio_access_key',None)
        minioSecretKey = data.get('minio_secret_key',None)
        minioSecure = False
        client = InitializeClient(minioEndpoint, minioAccessKey, minioSecretKey, minioSecure)
        
        backupPath = data.get("file_path",None)
        bucketName = data.get("bucket_name",None)
        localPath = data.get("local_path",None)
        
        isRemote = data.get("remote",False)
        remoteHost = data.get("remote_host",None)
        remoteUser = data.get("remote_user",None)
        remotePassword = data.get("remote_password",None)
        
        if bucketName:
            if UploadFiles(client, bucketName, backupPath, isRemote, remoteHost, remoteUser, remotePassword):
                payload = {
                    "status":True,
                    "message":"Files restored to object store succesfully from path.",
                    "data":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status":False,
                    "message":"Files restore to object store failed from path.",
                    "data":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            if RestoreAllBuucketsFromRemote(client, isRemote, remoteHost, remoteUser, remotePassword, backupPath, localPath):
                payload = {
                    "status":True,
                    "message":"Files restored to object store succesfully from path.",
                    "data":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)