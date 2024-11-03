import re
import datetime
from .utils import *
from rest_framework import status
from elasticsearch import Elasticsearch
from rest_framework.views import APIView
from rest_framework.response import Response

class ViewIndexes(APIView):
    def get(self, request):
        elasticUrl = request.query_params.get('elastic_url',None)
        es = Elasticsearch(f'http://{elasticUrl}')
        
        try:
            IndexList = IndexListAndSize(es)
            indexSize=GetSizeOfIndex(es)
            payload = {
                "status":True,
                "message":"List of indexes in cluster",
                "data":IndexList,
                "total_size":indexSize,
                "error":None
            }
            return Response(payload, status=status.HTTP_200_OK)
        except Exception as e:
            payload = {
                "status":False,
                "message":"Error in listing indexes.",
                "data":str(e),
                "error":"Failed to fetch indexes."
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)


class BackupIndexes(APIView):
    def post(self, request):
        data = request.data

        elasticUrl = data.get('elastic_url',None)
        indexName = data.get("index_name",None)
        backupPath = data.get("backup_path",None)
        repoName = data.get("repo_name",None)
        snapshotName = data.get("snapshot_name",None)

        isRemote = data.get("remote",False)
        remoteHost = data.get("remote_host",None)
        remoteUser = data.get("remote_user",None)
        remotePort = data.get("remote_port",None)
        remotePassword = data.get("remote_password",None)
        
        es = Elasticsearch(f'http://{elasticUrl}')
        
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
                        indexSize=GetSizeOfIndex(es)
                        remoteSpace = CheckRemoteDiskSpace(sshclient, backupPath)
                        
                        indexSize = ConvertToBytesB(indexSize)
                        if isinstance(remoteSpace, str):
                            remoteSpace = ConvertToBytes(remoteSpace)
                        
                        if remoteSpace < indexSize:
                            payload = {
                                "status": False,
                                "message": "Not enough space on the remote host for backup.",
                                "required_space": FormatSize(indexSize),
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
        
        timestamp = f'{int(datetime.datetime.now().timestamp())}'
        if indexName:
            snapshotName = f'{timestamp}_snapshot_{indexName}'
        else:
            if not snapshotName:
                snapshotName = f'{timestamp}_snapshot'
            
        responseData= BackupToRemoteLocal(indexName, elasticUrl, repoName, snapshotName, isRemote, remotePort, remoteHost, remoteUser, remotePassword, backupPath)
        if responseData:
            payload = {
                    "status": True,
                    "message": 'Backup done.',
                    "data": responseData,
                    "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                    "status": False,
                    "message": 'Backup failed.',
                    "error": None
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
            
    def get(self, request):
        elasticUrl = request.query_params.get('elastic_url',None)
        es = Elasticsearch([elasticUrl])
        indexName = request.query_params.get("index_name",None)
        if indexName:
            indexSize=GetSizeOfIndex(es, indexName)
            payload = {
                "status": True,
                "message": 'Estimated size.',
                "size": indexSize,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            indexSize=GetSizeOfIndex(es)
            payload = {
                "status": True,
                "message": 'Estimated size.',
                "size": indexSize,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
          

class RestoreIndexesFromRemote(APIView):
    def post(self, request):
        data = request.data 
        backupPath = data.get("backup_path", None)
        remoteHost = data.get("remote_host", None)
        remoteUser = data.get("remote_user", None)
        remotePort = data.get("remote_port", None)
        remotePassword = data.get("remote_password", None)
        
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
                response = CopySnapshotFromRemote(remoteHost, remotePort, remoteUser, remotePassword, backupPath)
                if response:
                    payload = {
                        "status": True,
                        "message": 'Copied Snapshot files to elastic path.',
                        "path":backupPath,
                        "error": None
                    }
                    return Response(payload,status=status.HTTP_200_OK)
                else:
                    payload = {
                        "status": False,
                        "message": 'Backup failed.',
                        "data": None,
                        "path": backupPath,
                        "error": "Error restoring snapshot files."
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

    def put(self, request):
        elasticUrl = request.data.get('elastic_url',None)
        es = Elasticsearch([elasticUrl])
        indexes = request.data.get("index_name", [])
        responses = []
        valid_index_name_pattern = re.compile(r'^[a-zA-Z0-9_]+$') 
        for indexeName in indexes:
            if not valid_index_name_pattern.match(indexeName):
                responses.append({
                    "status": False,
                    "message": "Invalid index name. Must only contain letters, numbers, and underscores.",
                    "index": indexeName,
                    "error": None
                })
                continue 
            
            try:
                if es.indices.exists(index=indexeName):
                    responses.append({
                        "status": False,
                        "message": "Index already exists.",
                        "index": indexeName,
                        "error": None
                    })
                    continue
                
                es.indices.create(index=indexeName)
                payload={
                    "status": True,
                    "message": "Index created successfully.",
                    "index": indexeName,
                    "error": None
                }
                return Response(payload, status=status.HTTP_201_CREATED)

            except Exception as e:
                responses.append({
                    "status": False,
                    "message": "Index creation failed.",
                    "index": indexeName,
                    "error": str(e)
                })

        return Response(responses, status=status.HTTP_200_OK)


class RegisterSnapshotRepository(APIView):
    def get(self, request):
        elasticUrl = request.query_params.get('elastic_url',None)
        repositoryName = request.query_params.get("repository_name", None)
        snapshotName = request.query_params.get('snapshot_name',None)
        
        availableSnaps= ListAvailableSnapshots(elasticUrl, repositoryName, snapshotName)
        if availableSnaps:
            payload = {
                "status": True,
                "message": 'List of available snapshots.',
                "data": availableSnaps,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status": False,
                "message": 'List of available snapshots.',
                "data": availableSnaps,
                "error": "Error occurred."
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

    def post(self, request):
        repositoryName = request.data.get("repository_name", None)
        elasticUrl = request.data.get('elastic_url',None)
        
        resCode = RegisterSnapshotDirectory(elasticUrl, repositoryName)
        if resCode ==200:    
            payload = {
                "status":True,
                "message":"Repository registerd for restoration.",
                "error":None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status": False,
                "message": 'Snapshot registry failed.',
                "data": None,
                "error": "Snapshot registry failed."
            }
            return Response(payload,status=status.HTTP_400_BAD_REQUEST)


class RestoreSnapshots(APIView):
    def post(self, request):
        data = request.data

        elasticUrl = data.get('elastic_url',None)
        indexName = data.get("index_name",None)
        repoName = data.get("repo_name",None)
        snapshotName = data.get("snapshot_name",None)
        
        responseData = RestoreSnapshotsFromElasticPath(indexName, elasticUrl, repoName, snapshotName)
        if responseData:
            payload = {
                "status": True,
                "message": "Restore Done.",
                "data": responseData,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status": True,
                "message": "Restore failed.",
                "data": responseData,
                "error": None
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
