import os
import datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .utils import *
import psycopg2


class PostgresBackup(APIView):
    def get(self, request):
        postgresHost= request.query_params.get("postgres_host",None)
        postgresPort=request.query_params.get("postgres_port",None)
        postgresUser=request.query_params.get("postgres_user",None)
        postgresPassword=request.query_params.get("postgres_password",None)
        
        conn = psycopg2.connect(
            dbname = "postgres",
            user = postgresUser,
            password = postgresPassword,
            host = postgresHost,
            port = postgresPort
        )
        cur = conn.cursor()
        
        # Fetch databases
        cur.execute("SELECT datname, pg_database_size(datname) AS size_in_bytes FROM pg_database WHERE datistemplate = false;")
        databases = cur.fetchall()
        cur.execute("""
            SELECT SUM(pg_database_size(datname)) AS total_size_in_bytes
            FROM pg_database
            WHERE datistemplate = false;
        """)
        total_size = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        result = []
        for db in databases:
            database_name = db[0]
            size_in_bytes = db[1]
            estimated_size = FormatSize(size_in_bytes)  # Convert size to human-readable format

            result.append({
                "database_name": database_name,
                "estimated_size": estimated_size
            })
        total_size_human_readable = FormatSize(int(total_size))
        payload = {
            "status":True,
            "message":"List of Databases",
            "data":result,
            "total_size":total_size_human_readable,
            "error":None
        }
        return Response(payload, status=status.HTTP_200_OK)
    
    def post(self, request):
        data = request.data
        
        postgresHost= data.get("postgres_host",None)
        postgresPort=data.get("postgres_port",None)
        postgresUser=data.get("postgres_user",None)
        postgresPassword=data.get("postgres_password",None)
        
        backupPath = data.get("backup_path",None)
        backupType = data.get("backup_type",None)
        dbName = data.get("database_name",None)
        startTime = data.get('start_time',None)
        endTime = data.get('end_time',None)
        
        remoteHost = data.get('remote_host',None)
        remoteUser = data.get('remote_user',None)
        remotePort = data.get('remote_port',None)
        remotePassword = data.get('remote_password',None)
        isRemote = data.get('remote',None)

        localPath = "/tmp/postgresbackups"
        
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
                        conn = psycopg2.connect(
                            dbname = "postgres",
                            user = postgresUser,
                            password = postgresPassword,
                            host = postgresHost,
                            port = postgresPort
                        )
                        cur = conn.cursor()
                        
                        # Fetch databases
                        cur.execute("SELECT datname, pg_database_size(datname) AS size_in_bytes FROM pg_database WHERE datistemplate = false;")
                        databases = cur.fetchall()
                        cur.execute("""
                            SELECT SUM(pg_database_size(datname)) AS total_size_in_bytes
                            FROM pg_database
                            WHERE datistemplate = false;
                        """)
                        total_size = cur.fetchone()[0]
                        
                        cur.close()
                        conn.close()
                        
                        result = []
                        for db in databases:
                            database_name = db[0]
                            size_in_bytes = db[1]
                            estimated_size = FormatSize(size_in_bytes)  # Convert size to human-readable format

                            result.append({
                                "database_name": database_name,
                                "estimated_size": estimated_size
                            })
                        totalSizeInBytes = FormatSize(int(total_size))
                        remoteSpace = CheckRemoteDiskSpace(sshclient, backupPath)
                        
                        if isinstance(totalSizeInBytes, str):
                            totalSizeInBytes = ConvertToBytesB(totalSizeInBytes)
                        if isinstance(remoteSpace, str):
                            remoteSpace = ConvertToBytes(remoteSpace)
                            
                        if remoteSpace < totalSizeInBytes:
                            payload = {
                                "status": False,
                                "message": "Not enough space on the remote host for backup.",
                                "required_space": FormatSize(totalSizeInBytes),
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
        
        if backupPath:
            backupPath = f'{backupPath}/{int(datetime.datetime.now().timestamp())}_Postgres_Backup'
                
        if backupType.lower() == "server":
            schemaPath = ServerSchemaBackup(postgresUser, postgresHost, postgresPort, postgresPassword, backupPath, localPath, isRemote, remoteHost, remoteUser, remotePassword)
            dataPath =  ServerDataBackup(postgresUser, postgresHost, postgresPort, postgresPassword, backupPath, localPath, isRemote, remoteHost, remoteUser, remotePassword)
            if schemaPath and dataPath:
                payload = {
                    "status":True,
                    "message":"Backup successfull.",
                    "schemaFilePath": schemaPath,
                    "dataFilePath":dataPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status": False,
                    "message": "Backup failed.",
                    "schemaFilePath": schemaPath,
                    "dataFilePath": dataPath,
                    "error": "Backup operation failed"
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
            
        elif backupType.lower() == "database":
            if isRemote:
                # Backing up data with queries
                result = BackupCaseQueryRemote(startTime, endTime, postgresUser, postgresHost, postgresPort, postgresPassword, dbName, backupPath, remoteHost, remoteUser, remotePassword)
                if any(item["status"] is False for item in result):
                    return Response({
                        "status": False,
                        "message": "Backup failed.",
                        "error": result[0]["error"]  # Provide the first error encountered
                    }, status=status.HTTP_400_BAD_REQUEST)

                # Return success response
                return Response({
                    "status": True,
                    "message": "Backup successful.",
                    "backup_path": backupPath,
                    "error": None
                }, status=status.HTTP_200_OK)    
            else:
                if startTime is not None and endTime is not None:
                    queryResults = LocalCaseQuery(startTime, endTime, postgresUser, postgresHost, postgresPort, postgresPassword, dbName, localPath)
                    for query_info in queryResults:
                        RunPsql(query_info["query"], query_info["output_file"], postgresUser, postgresHost, postgresPort, dbName)    
                    
                    return Response({
                        "status":True,
                        "message":"Backup Successfull.",
                        "backup_path":localPath,
                        "error":None
                    }, status=status.HTTP_200_OK)    
                else:
                    return Response({
                        "status":False,
                        "message":"Please provide starttime and endtime for partial backup.",
                        "data":None,
                        "error":"Backup Failed."
                    }, status=status.HTTP_400_BAD_REQUEST)


class PostgresRestoreServer(APIView):
    def post(self, request):
        data = request.data
        
        postgresHost= data.get("postgres_host",None)
        postgresPort=data.get("postgres_port",None)
        postgresUser=data.get("postgres_user",None)
        postgresPassword=data.get("postgres_password",None)
        
        filePath = data.get("file_path",None)
        schemaPath = data.get("schema_path",None)

        remoteHost = data.get('remote_host',None)
        remoteUser = data.get('remote_user',None)
        remotePort = data.get('remote_port',None)
        remotePassword = data.get('remote_password',None)
        isRemote = data.get('remote',None)
        
        if not isRemote:
            # Restore for local
            if filePath and schemaPath:
                schemaPath =  ServerSchemaRestore(postgresUser, postgresHost, postgresPort, postgresPassword, filePath)
                dataPath = ServerDataRestore(postgresUser, postgresHost, postgresPort, postgresPassword, schemaPath)
                if schemaPath and dataPath:
                    payload = {
                        "status":True,
                        "message":"Server restored successfully from path.",
                        "schema_path":schemaPath,
                        "data_path":dataPath,
                        "error":None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
                else:
                    payload = {
                        "status":False,
                        "message":"Server restoration failed.",
                        "data":None,
                        "error":"Error creating databases and schema."
                    }
                    return Response(payload, status=status.HTTP_400_BAD_REQUEST)
            else:
                payload = {
                    "status":False,
                    "message":"User has not provided schema and backup file path.",
                    "data":None,
                    "error":"Restoration will not proceed."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            if not (filePath and remoteHost and remotePort and remoteUser and remotePassword):
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
                    if RestoreServerFromRemote(remote_host=remoteHost, remote_user=remoteUser, remote_password=remotePassword, local_host=postgresHost, db_user=postgresUser, db_port=postgresPort, db_password=postgresPassword, schema_file_path=schemaPath, data_file_path=filePath):
                        return Response({
                            "status":True,
                            "message":"Server Restored Successfully",
                            "data":None,
                            "error":None
                        }, status=status.HTTP_200_OK)
                    else:
                        return Response({
                            "status":False,
                            "message":"Server Restored Failed",
                            "data":None,
                            "error":None
                        }, status=status.HTTP_400_BAD_REQUEST)
                else:
                    payload = {
                        "status": False,
                        "message": "Remote client connection failed.",
                        "data": None,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_404_NOT_FOUND)
            

class CaseMMRestoreSchemaWithData(APIView):
    def post(self, request):
        postgresHost= request.data.get("postgres_host",None)
        postgresPort=request.data.get("postgres_port",None)
        postgresUser=request.data.get("postgres_user",None)
        postgresPassword=request.data.get("postgres_password",None)
        
        schemaFilePath = request.data.get("schema_path",None)
        dataFilePath = request.data.get("csv_file_path",None)
        dbName = request.data.get("database_name",None)
        
        remoteHost = request.data.get('remote_host',None)
        remoteUser = request.data.get('remote_user',None)
        remotePassword = request.data.get('remote_password',None)
        isRemote = request.data.get('remote',None)
        
        if not isRemote:
            if schemaFilePath:
                # dbname = RestoreSchema(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, dbName, POSTGRES_PASSWORD, schemaFilePath)
                tableNames = ExtractTableNames(schemaFilePath)
                for tablename in tableNames:
                    for csv_file in os.listdir(dataFilePath):
                        if csv_file.endswith('.csv'):
                            if csv_file.replace('.csv', '') == tablename:
                                csv_file_path = os.path.join(dataFilePath, csv_file)
                                RestoreCaseQueryData(postgresUser, postgresHost, postgresPort, dbName, postgresPassword ,tablename, csv_file_path, schemaFilePath)

                return Response({
                    "status":True,
                    "message":"Schema Restored Successfully",
                    "dbname":dbName,
                    "error":None
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    "status":False,
                    "message":"Schema Path not provided.",
                    "data":None,
                    "error":"Schema restoration will not proceed."
                }, status=status.HTTP_400_BAD_REQUEST)
        else:
            if RestoreCaseQueryFromRemote(remoteHost, remoteUser, remotePassword, postgresHost, postgresUser, postgresPort, postgresPassword, dbName, schemaFilePath, dataFilePath):
                return Response({
                    "status":True,
                    "message":"Case Data Restored Successfully",
                    "dbname":dbName,
                    "error":None
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    "status":False,
                    "message":"Case Data Restored.",
                    "dbname":dbName,
                    "error":None
                }, status=status.HTTP_200_OK)

