from .views import *
import paramiko
from scp import SCPClient
import re
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
import os
import time
import subprocess
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

def CheckDirExists(ssh, path):
    # Check if the directory exists on the remote server
    command = f'if [ -d "{path}" ]; then echo "exists"; fi'
    stdin, stdout, stderr = ssh.exec_command(command)
    return stdout.read().decode().strip() == "exists"

def CheckForErrors(stdout, stderr):
    stdoutOutput = stdout.read().decode().strip()
    stderrOutput = stderr.read().decode().strip()
    if stderrOutput:
        print(f"Error: {stderrOutput}")
        return False
    else:
        if stdoutOutput:
            print(stdoutOutput)
        return True

def FormatSize(sizeInBytes):
    if sizeInBytes < 1024:
        return f"{sizeInBytes} B"
    elif sizeInBytes < 1024**2:
        return f"{sizeInBytes / 1024:.2f} KB"
    elif sizeInBytes < 1024**3:
        return f"{sizeInBytes / 1024**2:.2f} MB"
    elif sizeInBytes < 1024**4:
        return f"{sizeInBytes / 1024**3:.2f} GB"
    else:
        return f"{sizeInBytes / 1024**4:.2f} TB"

def ConvertToBytes(sizeStr):
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

def GetEstimatedBackupSize(sshClient, keySpaces):
    if isinstance(keySpaces, str):
        keySpaces = [keySpaces]
        
    backupSizeEstimates = {}
    totalBackupSize = 0

    try:
        for keySpace in keySpaces:
            print(keySpace)
            command = f'nodetool cfstats {keySpace}'
            stdin, stdout, stderr = sshClient.exec_command(command)

            stdoutOutput = stdout.read().decode()
            errorOutput = stderr.read().decode()

            if errorOutput:
                backupSizeEstimates[keySpace] = "0 B"
                continue

            totalSizeMatch = re.search(r'Space used \(total\):\s+(\d+)', stdoutOutput)
            
            if totalSizeMatch:
                totalSize = int(totalSizeMatch.group(1))
                formattedSize = FormatSize(totalSize) 
                backupSizeEstimates[keySpace] = formattedSize
                totalBackupSize += totalSize
            else:
                print(f"Could not find size information for keyspace '{keySpace}'.")
                backupSizeEstimates[keySpace] = "0 B"

    except Exception as e:
        print(f"Error estimating backup sizes: {e}")
        return None

    finally:
        formattedTotalSize = FormatSize(totalBackupSize)

    return backupSizeEstimates, formattedTotalSize

def KeyspaceExists(host, port, keyspace):
    cluster = Cluster([host], port=int(port))
    
    try:
        print(f"Checking keyspace")
        session = cluster.connect()
        query = f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace}'"
        result = session.execute(query)
        return len(result.current_rows) > 0
    except Exception as e:
        print(f"Error checking keyspace: {str(e)}")
        return False
    finally:
        cluster.shutdown()

def CheckTablesExist(host, username, password, keyspace, tableName):
    authProvider = PlainTextAuthProvider(username, password)
    cluster = Cluster([host], auth_provider=authProvider)
    
    try:
        print("Check table exists")
        session = cluster.connect(keyspace)  # Connect to the specified keyspace
        query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{tableName}'"
        result = session.execute(query)
        return len(result.current_rows) > 0
    except Exception as e:
        print(f"Error checking table: {str(e)}")
        return False
    finally:
        cluster.shutdown()

def GetTableUuid(host, keyspace, tablename):
    # Connect to the ScyllaDB cluster
    cluster = Cluster([host])
    session = cluster.connect()

    # Switch to the desired keyspace
    session.set_keyspace(keyspace)

    # Query to get the UUID of the table
    query = "SELECT id FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s"
    statement = SimpleStatement(query)
    result = session.execute(statement, (keyspace, tablename))

    # Close the connection
    cluster.shutdown()

    # Check if we got a result
    if result and len(result.current_rows) > 0:
        return result[0].id  # Assuming `table_id` returns the UUID
    else:
        return None

def StartScylla(host, username, password):
    try:
        sshclient = CreateSshClient(host, 22, username, password)
        command = f'echo {password} | sudo -S systemctl restart scylla-server'
        print("Restarting Scylla service...")
        stdin, stdout, stderr = sshclient.exec_command(command)
        CheckForErrors(stdout, stderr)  
        return True
    except Exception as e:
        print(e)

def CopyFilesToDestination(host, username, password, sourcePath):
    temp_path = "/tmp/scylla_tmp"
    try:
        sshClient = CreateSshClient(host, 22, username, password)
        stdin, stdout, stderr = sshClient.exec_command(f"mkdir -p {temp_path}")
        CheckForErrors(stdout, stderr)
        
        with SCPClient(sshClient.get_transport()) as scp:
            # List files in the local source directory
            local_files = os.listdir(sourcePath)
            # print("Files to copy:", local_files)

            for file in local_files:
                local_file_path = os.path.join(sourcePath, file)
                remote_file_path = os.path.join(temp_path, file)
                print(f"Copying {file} to {remote_file_path}...")
                # Copy file to the remote destination
                try:
                    scp.put(local_file_path, remote_file_path)
                    print(f"Successfully copied {file} to {remote_file_path}")
                except Exception as e:
                    print(f"Error copying {file}: {e}")
    except Exception as e:
        print(f"SSH connection failed: {e}")
        return False
    finally:
        sshClient.close()

def ChangeOwnership(host, username, password):
    try:
        sshClient = CreateSshClient(host, 22, username, password)
        tempPath = "/tmp/scylla_tmp"
        command = f'echo {password} | sudo -S chown scylla:scylla {tempPath}/*'
        stdin, stdout, stderr = sshClient.exec_command(command)
        CheckForErrors(stdout, stderr)
            
    except Exception as e:
        print(f"SSH connection failed: {e}")
        return False
    finally:
        if sshClient:
            sshClient.close()

def MoveFiles(host, username, password, keyspace, tablename):
    try:
        with CreateSshClient(host, 22, username, password) as sshClient:
            tempPath = "/tmp/scylla_tmp"
            uuid= GetTableUuid(host, keyspace, tablename)
            tableid = str(uuid).replace("-", "")
            destinationPath = f"/var/lib/scylla/data/{keyspace}/{tablename}-{tableid}"

            command = f'echo {password} | sudo -S mv "{tempPath}"/* "{destinationPath}"'
            stdin, stdout, stderr = sshClient.exec_command(command)
            CheckForErrors(stdout, stderr)
            command = f'echo {password} | sudo -S rm -rf "{tempPath}"'
            stdin, stdout, stderr = sshClient.exec_command(command)
            CheckForErrors(stdout, stderr)
            # stdin, stdout, stderr = sshClient.exec_command(f' rm -rf {tempPath}')
            # CheckForErrors(stdout, stderr)
            
    except Exception as e:
        print(f"SSH connection failed: {e}")
        return False

def CaptureDataForSingleTableLocalAndRemote(host, username, password, keyspace, tablename, backupPath, localPath, isRemote=False, remoteHost=None, remotePort=None, remoteUser=None, remotePassword=None):
    sshClient = CreateSshClient(host, 22, username, password)
    
    snapshot_tag = f"{tablename}_snapshot"
    command = f"nodetool snapshot --tag {snapshot_tag} --table {tablename} {keyspace}"
    print("command", command)
    stdin, stdout, stderr = sshClient.exec_command(command)
    
    stdoutOutput = stdout.read().decode()
    errorOutput = stderr.read().decode()
    
    if errorOutput:
        print(f"Error during snapshot creation: {errorOutput}")
        return

    print(f"Snapshot created successfully: {stdoutOutput}")
    
    # Find the snapshot directory
    find_snapshot_command = f"find /var/lib/scylla/data/{keyspace}/{tablename}-*/snapshots/{snapshot_tag} -type d"
    
    stdin, stdout, stderr = sshClient.exec_command(find_snapshot_command)
    snapshot_dir = stdout.read().decode().strip()
    errorOutput = stderr.read().decode()
    
    if errorOutput or not snapshot_dir:
        print(f"Error finding snapshot directory: {errorOutput}")
        raise Exception(f"Snapshot directory not found: {errorOutput}")

    print(f"Snapshot directory found: {snapshot_dir}")
    
    # Create an SFTP client for local or remote transfer
    scpClient = paramiko.SFTPClient.from_transport(sshClient.get_transport())
    
    if isRemote:
        # Handle remote backup to a different machine
        remoteSshClient = CreateSshClient(remoteHost, remotePort, remoteUser, remotePassword)
        remoteSftpClient = paramiko.SFTPClient.from_transport(remoteSshClient.get_transport())
        
        backupPath = f'{backupPath}/{int(datetime.datetime.now().timestamp())}_Scylla_Backup'
        CreateRemoteDir(remoteSshClient, backupPath)

        # Transfer each snapshot file to the remote backup machine
        for file in scpClient.listdir(snapshot_dir):
            remote_file_path = f"{snapshot_dir}/{file}"
            with scpClient.file(remote_file_path, 'rb') as file_obj:
                remote_backup_file_path = os.path.join(backupPath, file)
                with remoteSftpClient.file(remote_backup_file_path, 'wb') as remote_file_obj:
                    remote_file_obj.write(file_obj.read())
                    print(f"Copied {file} to remote: {remote_backup_file_path}")
        
        # Close the remote SFTP connection
        remoteSftpClient.close()
        remoteSshClient.close()
    
    # else:
    #     # Handle local backup
    #     if not os.path.exists(localPath):
    #         os.makedirs(localPath)
        
    #     # Transfer each snapshot file to the local backup path
    #     for file in scpClient.listdir(snapshot_dir):
    #         remote_file_path = f"{snapshot_dir}/{file}"
    #         local_file_path = os.path.join(localPath, file)
    #         scpClient.get(remote_file_path, local_file_path)
    #         print(f"Copied {file} to {localPath}")
    
    # Close the SFTP and SSH connections
    scpClient.close()
    sshClient.close()
    
    print(f"Backup of table {tablename} completed successfully.")
    return backupPath if isRemote else snapshot_dir


def ListSnapshots(host, port, username, password, keyspace, table):
    sshClient= CreateSshClient(host, int(port), username, password)
    
    command = 'nodetool listsnapshots'
    stdin, stdout, stderr = sshClient.exec_command(command)
    
    stdoutOutput = stdout.read().decode()
    errorOutput = stderr.read().decode()
    
    if errorOutput:
        print(f"Error: {errorOutput}")
    
    filtered_snapshots = []
    for line in stdoutOutput.splitlines():
        if keyspace in line and table in line:
            parts = line.split() 
            if len(parts) >= 3:
                snapshot_keyspace = parts[1]
                snapshot_table = parts[2]
                snapshot_size = " ".join(parts[3:]).strip()

                # Match against provided keyspace_name and table_name
                if snapshot_keyspace == keyspace and snapshot_table == table:
                    snapshot_info = {
                        "snapshot_name": parts[0], 
                        "keyspace": snapshot_keyspace,
                        "table": snapshot_table,
                        "size": snapshot_size.split()[-2] + " " + snapshot_size.split()[-1]
                    }
                    filtered_snapshots.append(snapshot_info)
    
    if filtered_snapshots is None:
        print(f"No snapshots found for keyspace '{keyspace}' and table '{table}'.")
        return None
    
    return filtered_snapshots

#needed for restore single table from local
def RestoreDataForSingleTableLocal(host, scyllaport, username, password, keyspace, tablename, snapshotname, scylla_data_dir='/var/lib/scylla/data'):
    try:
        sshClient= CreateSshClient(host, 22, username, password)
        
        if KeyspaceExists(host, scyllaport, keyspace):
            if CheckTablesExist(host, username, password, keyspace, tablename):
        
                table_uuid = GetTableUuid(host, keyspace, tablename)
                tableid = str(table_uuid).replace("-", "")
                dataDir = f'{scylla_data_dir}/{keyspace}/{tablename}-{tableid}'
        
                snapshot_dir = os.path.join(dataDir, 'snapshots', snapshotname)
                
                print("Snapshot Directory",snapshot_dir)
                print("Data Directory:", dataDir)

                print(f"Truncating table {keyspace}.{tablename}...")
                truncate_command = f'cqlsh {host} -e "TRUNCATE {keyspace}.{tablename};"'
                stdin, stdout, stderr = sshClient.exec_command(truncate_command)
                CheckForErrors(stdout, stderr)
                
                print(f"Copying snapshot files from {snapshot_dir} to {dataDir}...")
                copyCommand = f"echo {password} | sudo -S cp -r {snapshot_dir}/* {dataDir}/"
                stdin, stdout, stderr = sshClient.exec_command(copyCommand)
                CheckForErrors(stdout, stderr)
                
                changeOwner = f"echo {password} | sudo -S chown scylla:scylla {dataDir}/*"
                stdin, stdout, stderr = sshClient.exec_command(changeOwner)
                CheckForErrors(stdout, stderr)

                print(f"Snapshot {snapshotname} restored successfully to {keyspace}.{tablename}")
            
                return True
        
    except Exception as e:
        print(f"An error occurred during restoration: {e}")
        return False
    finally:
        if sshClient:
            sshClient.close() 

def MoveFilesRemoteToScylla(scyllaSshClient, keyspace, tablename, backupPath, remoteHost, remoteUser, remotePassword):
    try:
        remoteSshClient = CreateSshClient(remoteHost, 22, remoteUser, remotePassword)
        remoteSftpClient = paramiko.SFTPClient.from_transport(remoteSshClient.get_transport())

        target_path_base = f"/var/lib/scylla/data/{keyspace}/{tablename}-*/"
        stdin, stdout, stderr = scyllaSshClient.exec_command(f"sudo mkdir -p {target_path_base}")
        CheckForErrors(stdout, stderr)

        print(f"Target directory found/created on ScyllaDB host: {target_path_base}")
        
        remote_files = remoteSftpClient.listdir(backupPath)
        temp_path = "/tmp/scylla_tmp/"
        
        stdin, stdout, stderr = scyllaSshClient.exec_command(f"mkdir -p {temp_path}")
        CheckForErrors(stdout, stderr)

        scyllaSftpClient = paramiko.SFTPClient.from_transport(scyllaSshClient.get_transport())

        for remote_file in remote_files:
            remote_file_path = os.path.join(backupPath, remote_file)
            scylla_temp_file = os.path.join(temp_path, remote_file)  # Temp location on ScyllaDB machine
            
            print(f"Transferring {remote_file} from remote backup to ScyllaDB...")

            with remoteSftpClient.file(remote_file_path, 'rb') as remote_file_obj:
                with scyllaSftpClient.file(scylla_temp_file, 'wb') as scylla_file_obj:
                    scylla_file_obj.write(remote_file_obj.read())
                    print(f"Transferred {remote_file} to ScyllaDB at {scylla_temp_file}")

            print(f"Moved {remote_file} to {target_path_base}")

        remoteSftpClient.close()
        scyllaSftpClient.close()

        return True
    
    except Exception as e:
        print(f"An error occurred during file movement: {e}")
        return False
    
    finally:
        if remoteSshClient:
            remoteSshClient.close()
        if scyllaSshClient:
            scyllaSshClient.close()

def RestoreDataForSingleTableLocalAndRemote(host, port, username, password, keyspace, tablename, backupPath, isRemote=False,remoteHost=None, remoteUser=None, remotePassword=None):
    try:
        # Create an SSH client for the target ScyllaDB host
        sshClient = CreateSshClient(host, 22, username, password)
        
        # Check if the keyspace and table exist
        if KeyspaceExists(host, port, keyspace):
            if CheckTablesExist(host, username, password, keyspace, tablename):
                if isRemote:
                    MoveFilesRemoteToScylla(sshClient,keyspace,tablename,backupPath,remoteHost,remoteUser,remotePassword)
                    ChangeOwnership(host,username,password)
                    MoveFiles(host,username,password,keyspace,tablename)
                # else:
                #     CopyFilesToDestination(host, username, password, localPath)
                #     time.sleep(2)
                #     ChangeOwnership(host, username, password)
                #     time.sleep(2)
                #     MoveFiles(host, username, password, keyspace, tablename)
                
                #     print("Data restoration completed successfully.")
                    return True
            else:
                print(f"Table not found")
                return False
        else:
            print(f"Keyspace not found")
            return False
    
    except Exception as e:
        print(f"An error occurred during restoration: {e}")
        return False
    
    finally:
        sshClient.close()
    
    # return True

def RestoreKeySpaceFromLocal(hostIP, scyllaPort, username, password, keySpace, scylla_data_dir='/var/lib/scylla/data'):
    sshClient = CreateSshClient(hostIP,22,username,password)
    sftp = sshClient.open_sftp()
    try:

        for keyspace in keySpace:
            keyspace_path = os.path.join(scylla_data_dir, keyspace)
            
            if not KeyspaceExists(hostIP, scyllaPort, keyspace):
                print(f"keyspace {keyspace} does not exist creating it")
                CreatNewKeyspace(hostIP, scyllaPort, 22, username, password, keyspace)
                
            for table_folder in sftp.listdir(keyspace_path):
                table_path = os.path.join(keyspace_path, table_folder)
                
                if '-' not in table_folder:
                    continue

                # Extract the table name from the folder name
                tablename = table_folder.split('-')[0]
                snapshots_path = os.path.join(table_path, 'snapshots')

                # Identify the latest snapshot by getting the most recent directory
                snapshots = sorted(sftp.listdir(snapshots_path))
                if not snapshots:
                    print(f"No snapshots found in {snapshots_path} for table {keyspace}.{tablename}.")
                    continue
                
                snapshot_name = snapshots[-1]
                snapshot_dir = os.path.join(snapshots_path, snapshot_name)
                
                for cql_file in sftp.listdir(snapshot_dir):
                    if cql_file.endswith('.cql'):
                        cql_file_path = os.path.join(snapshot_dir, cql_file)
                        print(f"Found schema file: {cql_file_path}")

                        # Read the CQL file from the snapshot and execute directly
                        with sftp.file(cql_file_path, 'r') as file:
                            cql_content = file.read().decode('utf-8').replace('\n', ' ').strip()  # Read and decode the file contents

                        print(f"Executing CQL directly for table {keyspace}.{tablename}...")

                        # Execute the CQL content directly via cqlsh
                        escaped_cql_content = cql_content.replace('"', '\\"')
                        exec_cql_command = f'echo "{escaped_cql_content}" | cqlsh {hostIP}'
                        stdin, stdout, stderr = sshClient.exec_command(exec_cql_command)
                        CheckForErrors(stdout, stderr)
                        
                        table_uuid = GetTableUuid(hostIP, keyspace, tablename)
                        tableid = str(table_uuid).replace("-", "")
                        table_data_dir = os.path.join(scylla_data_dir, keyspace, f"{tablename}-{tableid}")
                    
                print(f"Truncating table {keyspace}.{tablename}...")
                truncate_command = f'cqlsh {hostIP} -e "TRUNCATE {keyspace}.{tablename};"'
                stdin, stdout, stderr = sshClient.exec_command(truncate_command)
                CheckForErrors(stdout, stderr)

                print(f"Copying snapshot files from {snapshot_dir} to {table_data_dir}...")
                for file in sftp.listdir(snapshot_dir):
                    src_file = os.path.join(snapshot_dir, file)
                    dst_file = os.path.join(table_data_dir, file)
                    
                    copy_command = f"echo '{password}' | sudo -S cp '{src_file}' '{dst_file}'"
                    stdin, stdout, stderr = sshClient.exec_command(copy_command)
                    CheckForErrors(stdout, stderr)
            
                changeOwner = f"echo {password} | sudo -S chown scylla:scylla {table_data_dir}/*"
                stdin, stdout, stderr = sshClient.exec_command(changeOwner)
                CheckForErrors(stdout, stderr)
                print(f"Snapshot {snapshot_name} restored successfully to {keyspace}.{tablename}") 
                
    except Exception as e:
        print("Exception occurred while restoring scylla data locally",str(e))
        return False
    
    finally:
        sftp.close()
    return True   

def CaptureKeySpaceSnapshotRemoteAndLocal(scyllaHost, scyllaUser, scyllaPassword, keySpaces, isRemote, localPath, backupPath=None, remoteHost=None, remotePort=None, remoteUsername=None, remotePassword=None):
    snapshotResults = {}
    
    try:
        # SSH connection to the ScyllaDB host
        sshClient = CreateSshClient(scyllaHost, 22, scyllaUser, scyllaPassword)
        sftpClient = sshClient.open_sftp()
        
        # Optional SSH connection to the remote backup server (if provided)
        if remoteHost and remoteUsername and remotePassword:
            remoteSshClient = CreateSshClient(remoteHost, int(remotePort), remoteUsername, remotePassword)
            remoteSftpClient = remoteSshClient.open_sftp()

        for keySpace in keySpaces:
            command = f'nodetool snapshot -t {keySpace} {keySpace}'
            stdin, stdout, stderr = sshClient.exec_command(command)
            
            stdoutOutput = stdout.read().decode()
            errorOutput = stderr.read().decode()

            if errorOutput:
                print("Error while backup keyspaces",errorOutput)

            snapshotIdMatch = re.search(r'snapshot name \[(\S+)\]', stdoutOutput)
            if snapshotIdMatch:
                snapshotId = snapshotIdMatch.group(1)
                print(f"Snapshot for keyspace '{keySpace}' taken successfully. Snapshot ID: {snapshotId}")
                
                basePath = f"/var/lib/scylla/data/{keySpace}/"
                
                # List all tables in the keyspace
                listTablesCommand = f'ls {basePath}'
                stdin, stdout, stderr = sshClient.exec_command(listTablesCommand)
                tablePaths = stdout.read().decode().splitlines()
                
                snapshotPaths = []
                # localSnapshotPaths = []
                for tablePath in tablePaths:
                    tableUUIDMatch = re.search(r'-(\S+)', tablePath)
                    if tableUUIDMatch:
                        tableUUID = tableUUIDMatch.group(1)
                        
                    # Construct the path to the snapshot for each table
                    snapshotPath = f"{basePath}{tablePath}/snapshots/{snapshotId}/"
                    if CheckDirExists(sshClient, snapshotPath):
                        snapshotPaths.append((snapshotPath, tableUUID))
                        
                        if backupPath:
                            if isRemote:
                                # If a remote backup path is provided, use the remote SFTP client to transfer
                                remoteTableBackupPath = os.path.join(backupPath, keySpace, tablePath)
                                CreateRemoteDir(remoteSshClient, remoteTableBackupPath)  # Create the directory on remote server

                                # Copy each file from the source machine to the remote backup machine
                                remoteFiles = sftpClient.listdir(snapshotPath)
                                for remoteFile in remoteFiles:
                                    remoteFilePath = os.path.join(snapshotPath, remoteFile)
                                    remoteDestPath = os.path.join(remoteTableBackupPath, remoteFile)
                                    sftpClient.get(remoteFilePath, '/tmp/temp_snapshot_file')  # Download to temp on local
                                    remoteSftpClient.put('/tmp/temp_snapshot_file', remoteDestPath)  # Upload to remote
                                    print(f"Transferred {remoteFilePath} to {remoteDestPath}")

                            # else:
                            #     # Local backup if no remote server details are provided
                            #     localTableBackupPath = os.path.join(localPath, keySpace, tablePath, "snapshots", snapshotId)
                            #     os.makedirs(localTableBackupPath, exist_ok=True)

                            #     # Copy each file from the remote snapshot directory to the local machine
                            #     remoteFiles = sftpClient.listdir(snapshotPath)
                            #     for remoteFile in remoteFiles:
                            #         remoteFilePath = os.path.join(snapshotPath, remoteFile)
                            #         localFilePath = os.path.join(localTableBackupPath, remoteFile)
                            #         sftpClient.get(remoteFilePath, localFilePath)  # Transfer file locally
                            #         print(f"Transferred {remoteFilePath} to {localFilePath}")
                                # localSnapshotPaths.append(localTableBackupPath)

                print("Snapshot Path: ",snapshotPaths)
                if isRemote:
                    snapshotResults["remote path"] = backupPath
                else:
                    snapshotResults[keySpace] = snapshotPaths
                
            else:
                print("Error: Snapshot directory not found in the output.")
                snapshotResults = None
        
        return snapshotResults

    except Exception as e:
        print(f"Error taking remote snapshot: {e}")
        return None

    finally:
        sshClient.close()
        sftpClient.close()
        if isRemote:
            remoteSftpClient.close()
            remoteSshClient.close()

def KeyspaceExistsRemote(scyllaHost, scyllaUser, scyllaPassword, keyspace):
    try:
        sshClient = CreateSshClient(scyllaHost, 22, scyllaUser, scyllaPassword)
        # Check if the keyspace exists
        checkKeyspaceCommand = f"cqlsh {scyllaHost} -e \"DESCRIBE KEYSPACE {keyspace};\""
        stdin, stdout, stderr = sshClient.exec_command(checkKeyspaceCommand)
        stderr_output = stderr.read().decode().strip()

        # If there's no output in stderr, it means the keyspace exists
        if stderr_output == '':
            return True
        return False
    except Exception as e:
        print(f"Error checking if keyspace exists: {e}")
        return False
    finally:
        sshClient.close()

def TableExists(host, username, password, keyspace, table):
    try:
        # Create an SSH client
        sshClient = CreateSshClient(host, 22, username, password)
        # Check if the table exists
        checkTableCommand = f"cqlsh {host} -e \"SELECT * FROM {keyspace}.{table} LIMIT 1;\""
        stdin, stdout, stderr = sshClient.exec_command(checkTableCommand)
        stderr_output = stderr.read().decode().strip()

        # If there's no output in stderr, it means the table exists
        return stderr_output == ''
    
    except Exception as e:
        print(f"Error checking if table '{table}' exists: {e}")
        return False
    finally:
        sshClient.close()

# Function to execute schema file on Scylla host
def ExecuteSchemaFileOnScylla(scyllaHost, scyllaUser, scyllaPassword, local_schema_file):
    try:
        scyllaSshClient = CreateSshClient(scyllaHost, 22, scyllaUser, scyllaPassword)
        scp_client = SCPClient(scyllaSshClient.get_transport())
        
        # Upload schema file to the Scylla host
        remote_schema_path = f"/tmp/{os.path.basename(local_schema_file)}"
        scp_client.put(local_schema_file, remote_schema_path)
        print(f"Uploaded schema file to {remote_schema_path} on {scyllaHost}")
        
        # Execute schema file using cqlsh on the Scylla host
        command = f"cqlsh {scyllaHost} -f {remote_schema_path}"
        stdin, stdout, stderr = scyllaSshClient.exec_command(command)
        
        result = stdout.read().decode()
        error = stderr.read().decode()
        
        if error:
            print(f"Error executing schema file: {error}")
        else:
            print(f"Schema file executed successfully: {result}")
    finally:
        scyllaSshClient.close()

def RestoreKeySpaceFromRemote(ScyllaHost, scyllaPort, username, password, isRemote, backupPath=None, remoteHost=None, remoteUsername=None, remotePassword=None):
    try:
        # SSH connection to the ScyllaDB host
        sshClient = CreateSshClient(ScyllaHost, 22, username, password)
        sftpClient = sshClient.open_sftp()

        # Optional SSH connection to the remote backup server (if provided)
        if isRemote and remoteHost and remoteUsername and remotePassword:
            remoteSshClient = CreateSshClient(remoteHost, 22, remoteUsername, remotePassword)
            remoteSftpClient = remoteSshClient.open_sftp()
        
            remoteDirs = remoteSftpClient.listdir(backupPath)

        for keySpace in remoteDirs:
            if not KeyspaceExistsRemote(ScyllaHost, username, password, keySpace):
                print(f"keyspace {keySpace} does not exist creating it")
                CreatNewKeyspace(ScyllaHost, scyllaPort, 22, username, password, keySpace)
            
            table_path = os.path.join(backupPath, keySpace)
            for table in remoteSftpClient.listdir(table_path):
                table_name = table.split('-')[0]
                
                schema_file_path = os.path.join(table_path, table, "schema.cql")
                try:
                    remoteSftpClient.stat(schema_file_path)  # Check if the schema file exists
                    print(f"Schema file found for table {table} in keyspace {keySpace}. Executing...")

                    # Download the schema file to a local temporary location
                    local_schema_file = f"/tmp/{table}_schema.cql"
                    remoteSftpClient.get(schema_file_path, local_schema_file)

                    # Execute the schema file on the ScyllaDB host
                    ExecuteSchemaFileOnScylla(ScyllaHost, username, password, local_schema_file)

                    table_uuid = GetTableUuid(ScyllaHost, keySpace, table_name)

                    if table_uuid:
                        print(f"Found UUID for table {table_name}: {table_uuid}")
                        tableid = str(table_uuid).replace("-", "")
                        table_data_dir = f"/var/lib/scylla/data/{keySpace}/{table_name}-{tableid}"
                        print("new tables uuid: ",table_data_dir)

                        datafile_path = os.path.join(table_path, table)
                        print("data file path: ",datafile_path)
                        
                        remote_table_dir = f"/tmp/scylla/{keySpace}/{table_name}/"
                        stdin, stdout, stderr = sshClient.exec_command(f"mkdir -p {remote_table_dir}")
                        CheckForErrors(stdout, stderr)
                        
                        for item in remoteSftpClient.listdir(datafile_path):
                            remote_item_path = os.path.join(datafile_path, item)
                            local_item_path = os.path.join(remote_table_dir, item)
                            
                            with remoteSftpClient.file(remote_item_path, 'rb') as remote_file_obj:
                                with sftpClient.file(local_item_path, 'wb') as scylla_file_obj:
                                    scylla_file_obj.write(remote_file_obj.read())
                                    print(f"Transferred {remote_item_path} to ScyllaDB at {local_item_path}")
                            
                        
                        time.sleep(2)
                        change_owner_command = f"echo {password} | sudo -S chown -R scylla:scylla {remote_table_dir}"
                        stdin, stdout, stderr = sshClient.exec_command(change_owner_command)
                        CheckForErrors(stdout, stderr)
                        print(f"Ownership changed to 'scylla' for {remote_table_dir}")

                        time.sleep(2)
                        # Move files to the actual ScyllaDB table directory
                        move_files_command = f"echo {password} | sudo -S mv {remote_table_dir}* {table_data_dir}/"
                        
                        stdin, stdout, stderr = sshClient.exec_command(move_files_command)
                        CheckForErrors(stdout, stderr)
                        print(f"Files moved to {table_data_dir}")
                        
                        cleanup_command = f"echo {password} | sudo -S rm -rf {remote_table_dir}"
                        stdin, stdout, stderr = sshClient.exec_command(cleanup_command)
                        CheckForErrors(stdout, stderr)
                        print(f"Temporary directory {remote_table_dir} removed")
                        
                        cleanup_command = f"echo {password} | sudo -S rm -rf {local_schema_file}"
                        remoteSshClient.exec_command(cleanup_command)
                        CheckForErrors(stdout, stderr)
                        print(f"Temporary directory {local_schema_file} removed")
                        
                except FileNotFoundError:
                    print(f"No schema.cql file found for table {table_name} in keyspace {keySpace}. Skipping...")
    
    except Exception as e:
        print(f"Error restoring snapshot: {e}")
    
    finally:
        sshClient.close()
        sftpClient.close()
        if isRemote:
            remoteSshClient.close()
            remoteSftpClient.close()

# Helper function to create directory on remote server
def CreateRemoteDir(sshClient, path):
    sshClient.exec_command(f'mkdir -p {path}')

def CreatNewKeyspace(host, scyllaPort, port, username, password, keyspace):
    try:
        if KeyspaceExists(host, scyllaPort, keyspace):
            print(f"Keyspace '{keyspace}' already exists.")
            return True
        
        sshClient = CreateSshClient(host, int(port), username, password)
        
        # Create the new keyspace
        createKeyspaceCommand = f"cqlsh {host} -e \"CREATE KEYSPACE {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 3}};\""
        stdin, stdout, stderr = sshClient.exec_command(createKeyspaceCommand)
        stderr_output = stderr.read().decode().strip()
        if stderr_output:
            raise Exception(f"Error creating keyspace: {stderr_output}")
        
        print(f"Keyspace '{keyspace}' created successfully.")
        return True

    except Exception as e:
        print(f"Error creating new keyspace: {e}")
        return False
    finally:
        sshClient.close()

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

def AvailableData(session, keyspaces):
    skip_if_contains = 'offset'
    availableData = []
    for keyspace in keyspaces:
        query_tables = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}';"
        tables = [row.table_name for row in session.execute(query_tables)]
        
        for table in tables:
            query_columns = f"SELECT column_name FROM system_schema.columns WHERE keyspace_name = '{keyspace}' AND table_name = '{table}';"
            columns = [row.column_name for row in session.execute(query_columns)]
            
            columns = [col for col in columns if col not in skip_if_contains not in col]

            if 'ingestion_timestamp' in columns:
                query = f"SELECT min(ingestion_timestamp), max(ingestion_timestamp) FROM {keyspace}.{table};"
                result = session.execute(query).one()
                minDate, maxDate = result
                
                if minDate and maxDate:
                    TimestampMin = datetime.datetime.fromtimestamp(minDate).strftime('%Y-%m-%d %H:%M:%S')
                    TimestampMax = datetime.datetime.fromtimestamp(maxDate).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"Keyspace: {keyspace}, Table: {table} -> Date range: {TimestampMin} to {TimestampMax}")
                    availableData.append({f"keyspace": keyspace, 
                                            "table": table,
                                            "date range": {"from":TimestampMin,
                                                        "to":TimestampMax}
                                            }) 
                else:
                    payload = {
                        "status":False,
                        "message":"No valid date range found.",
                        "data":None,
                        "error":None
                    }
                    return payload
            else:
                pass
            
    return availableData

