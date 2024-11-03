from .utils import *
import datetime
from rest_framework import status
from cassandra.cluster import Cluster
from rest_framework.views import APIView
from rest_framework.response import Response


class ScyllaBackupForSingleTable(APIView):
    def get(self, request):
        scyllaHost = request.query_params.get('scylla_host',None)
        scyllaPort = request.query_params.get('scylla_port',None)
        scyllaPassword = request.query_params.get('scylla_password',None)
        scyllaUser = request.query_params.get('scylla_user',None)
        
        try:
            cluster = Cluster([scyllaHost], port=int(scyllaPort))
            session = cluster.connect()
            keySpaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        except Exception as e:
            payload = {
                    "status": True,
                    "message": "Error connecting to Scylla",
                    "data":str(e),
                    "error": None
                }
            return Response(payload, status=status.HTTP_404_NOT_FOUND)
        
        excludeKeyspaces = ['system', 'system_schema', 'system_auth', 'system_distributed', 'system_traces','system_distributed_everywhere']
        
        keySpaceNames = []
        totalSize = 0
        try:
            sshClient = CreateSshClient(scyllaHost, 22, scyllaUser, scyllaPassword)
            for row in keySpaces:
                keySpaceName=row.keyspace_name
                if keySpaceName in excludeKeyspaces:
                    continue
                try:
                    estimatedSizeDict, _ = GetEstimatedBackupSize(sshClient, [keySpaceName])
                    estimatedSize = estimatedSizeDict.get(keySpaceName, "0 B")
                    sizeInBytes = ConvertToBytes(estimatedSize)
                    totalSize += sizeInBytes 
                except Exception as e:
                    estimatedSize = f"Error estimating size: {str(e)}"
                
                tables = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keySpaceName}'")
                    
                table_names = [table.table_name for table in tables]
                keySpaceNames.append({
                    'keyspace_name': keySpaceName,
                    'table_name':table_names,
                    'estimated_size': estimatedSize
                })
            
            formattedTotalSize = FormatSize(totalSize)
            payload = {
                    "status": True,
                    "message": "List of available keyspaces in the cluster",
                    "data": keySpaceNames,
                    "total_size": formattedTotalSize,
                    "error": None
                }
            return Response(payload, status=status.HTTP_200_OK)
        
        except Exception as e:
            print(e)
            
        finally:
            session.shutdown()
            cluster.shutdown()
                
    # function to check remote connection
    def put(self,request):
        data = request.data

        # backupPath = data.get("backup_path",None)
        remoteHost = data.get("remote_host",None)
        remotePort = data.get("remote_port",None)
        remoteUser = data.get("remote_user",None)
        remotePassword = data.get("remote_password",None)
        
        if (remoteHost is None) or (remoteUser is None) or (remotePassword is None):
            payload = {
                "status": False,
                "message": "Please provide remote credentials to proceed for backup.",
                "data": None,
                "error": "Backup wont proceed."
            }
            return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)
        else:
            sshclient = CreateSshClient(remoteHost, int(remotePort), remoteUser, remotePassword)
            if sshclient:
                payload = {
                    "status": True,
                    "message": "Remote client connected succesfully.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_202_ACCEPTED)
            else:
                payload = {
                    "status": False,
                    "message": "Remote client connection failed.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_404_NOT_FOUND)
        
        
    def post(self, request):
        data = request.data
        
        scyllaHost = data.get('scylla_host',None)
        scyllaPort = data.get('scylla_port',None)
        scyllaUser = data.get('scylla_username',None)
        scyllaPassword = data.get('scylla_password',None)
        
        keySpaceName = data.get("keyspace_name", None)
        tableName = data.get("table_name", None)
        backupPath = data.get("backup_path",None)
        localPath = "/tmp/ScyllaBackup"
        
        isRemote = data.get("remote",False)
        remoteHost = data.get("remote_host",None)
        remotePort = data.get("remote_port",None)
        remoteUser = data.get("remote_user",None)
        remotePassword = data.get("remote_password",None)
        
        if not (keySpaceName and tableName):
            payload = {
                "status": False,
                "message": "Backup not initiated.",
                "data": None,
                "error": "Either keyspace or table name not provided"
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

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
                scyllaClient = CreateSshClient(scyllaHost, 22, scyllaUser, scyllaPassword)
                
                if sshclient:
                    try:
                        estimatedSizeDict, _ = GetEstimatedBackupSize(scyllaClient, [keySpaceName])
                        estimatedSize = estimatedSizeDict.get(keySpaceName, "0 B")
                        totalSizeBytes = ConvertToBytes(estimatedSize)
                        availableSpaceBytes = ConvertToBytesB(CheckRemoteDiskSpace(sshclient, backupPath))
                        # availableSpaceBytes = ConvertToBytesB(availableSpaceBytes)

                        if isinstance(totalSizeBytes, str):
                            totalSizeBytes = ConvertToBytes(totalSizeBytes)
                        

                        if availableSpaceBytes < totalSizeBytes:
                            payload = {
                                "status": False,
                                "message": "Not enough space on the remote host for backup.",
                                "required_space": FormatSize(totalSizeBytes),
                                "available_space": FormatSize(availableSpaceBytes),
                                "error": None
                            }
                            return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)

                        snapShotPaths = CaptureDataForSingleTableLocalAndRemote(scyllaHost, scyllaUser, scyllaPassword, keySpaceName, tableName, backupPath, localPath, isRemote, remoteHost, int(remotePort), remoteUser, remotePassword)
                        payload = {
                            "status": True,
                            "message": "Remote backup done successfully",
                            "path": snapShotPaths,
                            "error": None
                        }
                        return Response(payload, status=status.HTTP_200_OK)
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
        
        else:        
            try:
                snapShotPaths = CaptureDataForSingleTableLocalAndRemote(scyllaHost, scyllaUser, scyllaPassword, keySpaceName, tableName, backupPath,localPath, isRemote, remoteHost, remotePort, remoteUser ,remotePassword)
                payload = {
                    "status": True,
                    "message": "Backup done successfully",
                    "path": snapShotPaths,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            except Exception as e:
                payload = {
                    "status": False,
                    "message": "Backup failed due to an error.",
                    "data": None,
                    "error": str(e)
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class ScyllaRestoreForSingleTable(APIView):
    def get(self, request):
        data = request.data

        scyllaHost = data.get('scylla_host',None)
        scyllaPort = data.get('scylla_port',None)
        scyllaPassword = data.get('scylla_password',None)
        scyllaUser = data.get('scylla_username',None)
        keyspace = data.get('keyspace_name',None)
        table = data.get('table_name',None)
    
        snapshotOutput = ListSnapshots(scyllaHost, scyllaPort, scyllaUser, scyllaPassword, keyspace, table)  
        if snapshotOutput:
            payload = {
                "status": True,
                "message": "Available snaphots",
                "data":snapshotOutput,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status": False,
                "message": "Snapshots unavailable",
                "error": None
            }
            return Response(payload, status=status.HTTP_404_NOT_FOUND)
            
            
    def post(self, request):
        data = request.data

        scyllaHost = data.get('scylla_host',None)
        scyllaPort = data.get('scylla_port',None)
        scyllaPassword = data.get('scylla_password',None)
        scyllaUser = data.get('scylla_username',None)
        
        backupPath = data.get("backup_file", None)
        keyspace = data.get("keyspace",None)
        tableName = data.get("tablename",None)
        snapshotname = data.get("snapshot_name",None)
        
        isRemote = request.data.get("remote",False)
        remoteHost = request.data.get("remote_host",None)
        remoteUser = request.data.get("remote_user",None)
        remotePort = request.data.get("remote_port",None)
        remotePassword = request.data.get("remote_password",None)
        
        if not (keyspace and tableName or snapshotname):
            payload = {
                "status": False,
                "message": "Backup not initiated.",
                "data": None,
                "error": "Either keyspace, table name or snapshotname not provided"
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

        if isRemote:
            if not (backupPath and remoteHost and remotePort and remoteUser and remotePassword):
                payload = {
                    "status": False,
                    "message": "Please provide remote credentials with backup path to proceed with restore.",
                    "data": None,
                    "error": "Restore won't proceed without all remote credentials."
                }
                return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                sshclient = CreateSshClient(remoteHost, int(remotePort), remoteUser, remotePassword)
                if sshclient:
                    try:
                        reponse = RestoreDataForSingleTableLocalAndRemote(scyllaHost, scyllaPort, scyllaUser, scyllaPassword, keyspace, tableName, backupPath, isRemote, remoteHost, remoteUser, remotePassword)
                        if reponse: 
                            payload = {
                                "status": True,
                                "message": f"Restoration of table {tableName} completed successfully. Please restart ScyllaDB to reflect the newly restored data.",
                                "data": None,
                                "error": None
                            }
                            return Response(payload, status=status.HTTP_200_OK)
                        else:
                            payload = {
                            "status": False,
                                "message": "Restoration failed",
                                "data":None,
                                "error": "Check if the keyspace and table name are correct."
                            }
                            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
                    except Exception as e:
                            payload = {
                                "status": False,
                                "message": "Restoration failed due to an error.",
                                "data": None,
                                "error": str(e),
                            }
                            return Response(status=status.HTTP_400_BAD_REQUEST)
                else:
                    payload = {
                        "status": False,
                        "message": "Remote client connection failed.",
                        "data": None,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_404_NOT_FOUND)
        else:
            reponse = RestoreDataForSingleTableLocal(scyllaHost, scyllaPort, scyllaUser, scyllaPassword, keyspace, tableName, snapshotname)#RestoreDataForSingleTableLocalAndRemote(scyllaHost, scyllaPort, scyllaUser, scyllaPassword, keyspace, tableName, backupPath,localPath, isRemote, remoteHost, remoteUser, remotePassword)
            if reponse: 
                payload = {
                    "status": True,
                    "message": f"Restoration of table {tableName} completed successfully. Please restart ScyllaDB to reflect the newly restored data.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                "status": False,
                    "message": "Restoration failed",
                    "data":None,
                    "error": "Check if the keyspace and table name are correct."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        

class ScyllaKeyspaceAndTable(APIView):
    def get(self, request):
        scyllaHost = request.query_params.get('scylla_host',None)
        scyllaPassword = request.query_params.get('scylla_password',None)
        scyllaUser = request.query_params.get('scylla_username',None)
        keySpaceName = request.query_params.get("keyspace_name", None)
        tableName = request.query_params.get("table_name", None)
        
        if keySpaceName and tableName:
            if KeyspaceExists(scyllaHost, scyllaUser, scyllaPassword, keySpaceName):
                if CheckTablesExist(scyllaHost, scyllaUser, scyllaPassword, keySpaceName, tableName):
                    payload = {
                        "status": True,
                        "message": "Table exists.",
                        "data": tableName,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
                else:
                    payload = {
                        "status": True,
                        "message": "Table does not exists.",
                        "data": tableName,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_400_BAD_REQUEST)
            else:
                payload = {
                    "status": False,
                    "message": "Keyspace does not exists.",
                    "data": keySpaceName,
                    "error": None
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                "status": False,
                "message": "Keyspace name and table name are required.",
                "data": None,
                "error": None
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
    
        
class ScyllaBackupKeyspace(APIView):
    def post(self, request):
        data = request.data
        
        scyllaHost = data.get('scylla_host',None)
        scyllaPort = data.get('scylla_port',None)
        scyllaPassword = data.get('scylla_password',None)
        scyllaUser = data.get('scylla_username',None)
        
        backupPath = data.get("backup_path",None)
        localPath = "/tmp/ScyllaBackup"
        
        isRemote = data.get("remote",False)
        remoteHost = data.get("remote_host",None)
        remotePort = data.get("remote_port",None)
        remoteUser = data.get("remote_user",None)
        remotePassword = data.get("remote_password",None)
        
        cluster = Cluster([scyllaHost], port=int(scyllaPort))
        session = cluster.connect()
        keySpaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        
        systemKeyspaces = ['system', 'system_schema', 'system_auth', 'system_distributed', 'system_traces','system_distributed_everywhere']
        keySpaceNames = []
        
        for row in keySpaces:
            keySpaceName=row.keyspace_name
            if keySpaceName not in systemKeyspaces:
                keySpaceNames.append(keySpaceName)
        
        if isRemote:
            if not (backupPath and remoteHost and remotePort and remoteUser and remotePassword):
                payload = {
                    "status": False,
                    "message": "Please provide remote credentials with backup path to proceed with restore.",
                    "data": None,
                    "error": "Restore won't proceed without all remote credentials."
                }
                return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                sshclient = CreateSshClient(remoteHost, int(remotePort), remoteUser, remotePassword)
                if sshclient:
                    if backupPath:
                        backupPath = f'{backupPath}/{int(datetime.datetime.now().timestamp())}_Scylla_Backup'
                    if keySpaceNames:
                        path = CaptureKeySpaceSnapshotRemoteAndLocal(scyllaHost, scyllaUser, scyllaPassword, keySpaceNames, isRemote, localPath, backupPath, remoteHost, remotePort, remoteUser, remotePassword)
                        payload = {
                            "status": True,
                            "message": "Backup done",
                            "data": path,
                            "error": None
                        }
                        return Response(payload, status=status.HTTP_200_OK)
                    else:
                        payload = {
                            "status": False,
                            "message": "Backup cannot proceed.",
                            "data": None,
                            "error": "keyspaces does not exist."
                        }
                        return Response(payload, status=status.HTTP_204_NO_CONTENT)
                else:
                    payload = {
                        "status": False,
                        "message": "Remote client connection failed.",
                        "data": None,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_404_NOT_FOUND)
                
        else:
            # if keySpaceNames:
            path = CaptureKeySpaceSnapshotRemoteAndLocal(scyllaHost, scyllaUser, scyllaPassword, keySpaceNames, isRemote, localPath, backupPath, remoteHost, remotePort, remoteUser, remotePassword)
            if path:
                payload = {
                    "status": True,
                    "message": "Backup done",
                    "data": path,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status": False,
                    "message": "Backup Failed.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class ScyllaRestoreKeyspace(APIView):
    def post(self, request):
        data = request.data

        scyllaHost = data.get('scylla_host',None)
        scyllaPort = data.get('scylla_port', None)
        scyllaPassword = data.get('scylla_password',None)
        scyllaUser = data.get('scylla_username',None)
        
        backupPath = data.get("backup_file",None)
        
        isRemote = data.get("remote",False)
        remoteHost = data.get("remote_host",None)
        remotePort = data.get("remote_port",None)
        remoteUser = data.get("remote_user",None)
        remotePassword = data.get("remote_password",None)
        
        cluster = Cluster([scyllaHost], port=int(scyllaPort))
        session = cluster.connect()
        keySpaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        
        keySpaceNames = ["test","restore_cdr_data"]
        systemKeyspaces = ['system', 'system_schema', 'system_auth', 'system_distributed', 'system_traces','system_distributed_everywhere']
        for row in keySpaces:
            keySpaceName=row.keyspace_name
            # if keySpaceName not in systemKeyspaces:
                # keySpaceNames.append(keySpaceName)
        
        if isRemote:
            if not (backupPath and remoteHost and remotePort and remoteUser and remotePassword):
                payload = {
                    "status": False,
                    "message": "Please provide remote credentials with backup path to proceed with restore.",
                    "data": None,
                    "error": "Restore won't proceed without all remote credentials."
                }
                return Response(payload, status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                sshclient = CreateSshClient(remoteHost, int(remotePort), remoteUser, remotePassword)
                if sshclient:
                    RestoreKeySpaceFromRemote(scyllaHost, scyllaPort, scyllaUser, scyllaPassword, isRemote, backupPath, remoteHost, remoteUser, remotePassword)
                    payload = {
                        "status": True,
                        "message": "Restore done from remote. Please restart ScyllaDB to reflect the newly backed-up data.",
                        "path": backupPath,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
                else:
                    payload = {
                        "status": False,
                        "message": "Remote client connection failed.",
                        "data": None,
                        "error": "Restore failed."
                    }
                    return Response(payload, status=status.HTTP_404_NOT_FOUND)

        else:
            response = RestoreKeySpaceFromLocal(scyllaHost, scyllaPort, scyllaUser, scyllaPassword, keySpaceNames)
            if response:
                payload = {
                    "status": True,
                    "message": "Restore done. Please restart ScyllaDB to reflect the newly backed-up data.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status": False,
                    "message": "Restoration Failed.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)

    
    def put(self, request):
        scyllaHost = request.data.get('scylla_host',None)
        scyllaPassword = request.data.get('scylla_password',None)
        scyllaUser = request.data.get('scylla_username',None)
        
        restart = request.data.get("restart",None)
        if restart:
            StartScylla(scyllaHost,scyllaUser,scyllaPassword)
            payload = {
                    "status": True,
                    "message": "Scylladb has been restarted.",
                    "data": None,
                    "error": None,
                }
            return Response(payload, status=status.HTTP_200_OK)
        
#Deletion of data
class ScyllaTruncate(APIView):
    def get(self, request):
        scyllaHost = request.query_params.get('scylla_host',None)
        scyllaPort = request.query_params.get('scylla_port',None)
        scyllaPassword = request.query_params.get('scylla_password',None)
        scyllaUser = request.query_params.get('scylla_user',None)
        
        if not (scyllaHost and scyllaPort and scyllaUser and scyllaPassword):
            payload = {
                "status": False,
                "message": "Please provide credentials to check availability of data.",
                "data": None,
                "error": "Credentials not provided."
            }
            return Response(payload, status=status.HTTP_404_NOT_FOUND)
        
        try:
            cluster = Cluster(contact_points=[scyllaHost],port=int(scyllaPort))
            session = cluster.connect()
        except Exception as e:
            print(e)
            payload = {
                "status": False,
                "message": "Unable to connect to ScyllaDb.",
                "data": None,
                "error": "Connection failed."
            }
            return Response(payload, status=status.HTTP_408_REQUEST_TIMEOUT)
        
        query_keyspaces = "SELECT keyspace_name FROM system_schema.keyspaces;"
        keyspaces = [row.keyspace_name for row in session.execute(query_keyspaces) if row.keyspace_name not in ['system', 'system_schema', 'system_auth', 'system_distributed', 'system_traces']]

        availableData = AvailableData(session, keyspaces)
        if availableData != []:
            payload = {
                "status":True,
                "message":"Available date range data in cluster",
                "data":availableData,
                "error":None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status":False,
                "message":"Failed to fetch available date range data",
                "data":availableData,
                "error":None
            }
            return Response(payload, status=status.HTTP_404_NOT_FOUND)

    def post(self, request):
        scyllaHost = request.query_params.get('scylla_host',None)
        scyllaPort = request.query_params.get('scylla_port',None)
        scyllaPassword = request.query_params.get('scylla_password',None)
        scyllaUser = request.query_params.get('scylla_user',None)
        
        if not (scyllaHost and scyllaPort and scyllaUser and scyllaPassword):
            payload = {
                "status": False,
                "message": "Please provide credentials to proceed with deletion of data.",
                "data": None,
                "error": "Credentials not provided."
            }
            return Response(payload, status=status.HTTP_404_NOT_FOUND)
        
        try:
            cluster = Cluster(contact_points=[scyllaHost],port=int(scyllaPort))
            session = cluster.connect()
        except Exception as e:
            print(e)
            payload = {
                "status": False,
                "message": "Unable to connect to ScyllaDb.",
                "data": None,
                "error": "Connection failed."
            }
            return Response(payload, status=status.HTTP_408_REQUEST_TIMEOUT)
        
        
        query_keyspaces = "SELECT keyspace_name FROM system_schema.keyspaces;"
        keyspaces = [row.keyspace_name for row in session.execute(query_keyspaces) if row.keyspace_name not in ['system', 'system_schema', 'system_auth', 'system_distributed', 'system_traces']]
        for keyspace in keyspaces:
            # Get all tables in the keyspace
            query_tables = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}';"
            tables = [row.table_name for row in session.execute(query_tables)]
            
            for table in tables:
                # Execute the TRUNCATE command for each table
                truncate_query = f"TRUNCATE {keyspace}.{table};"
                session.execute(truncate_query)
                print(f"Truncated table: {keyspace}.{table}")
