from .views import *
from math import log
import json
import paramiko
import requests
import subprocess
from scp import SCPClient


def CreateSshClient(server, port, user, password):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(server, port, username=user, password=password)
        return client
    except Exception as e:
        print(f"Error creating SSH client: {e}")
        return None

def human_readable_size(sizeInBytes):
    if sizeInBytes == 0:
        return "0 Bytes"
    sizeNames = ["Bytes", "KB", "MB", "GB", "TB"]
    index = min(int(log(sizeInBytes, 1024)), len(sizeNames) - 1)
    size = sizeInBytes / (1024 ** index)
    return f"{size:.2f} {sizeNames[index]}"

def IndexListAndSize(es):
    indexes = es.indices.get_alias(index='*')
    # indexList = list(indexes.keys())
    indexList = [index for index in indexes.keys() if not index.startswith('.')]  # Filter out indexes starting with '.'
    
    indexStats = es.indices.stats(index=indexList)
    resp=[]
    for index in indexList:
        indexSizeBytes = indexStats['indices'][index]['total']['store']['size_in_bytes']
        resp.append({
            "index": index,
            "estimated_size": human_readable_size(indexSizeBytes),
        })
    return resp

def GetSizeOfIndex(es, indexName=None):
    if indexName:
        indexStats = es.indices.stats(index=indexName)
    else:
        indexStats = es.indices.stats()
        
    totalSize = sum(
        stat['total']['store']['size_in_bytes'] 
        for stat in indexStats['indices'].values()
    )
    Size = human_readable_size(totalSize)
    return Size

def BackupToRemoteLocal(indexName, elasticUrl, repoName, snapshotName, isRemote, remotePort, remoteHost, remoteUser, remotePassword, remoteBackupPath):
    elasticBackupPath = "/mnt/backups/"
    
    if indexName:
        respone = RegisterSnapshotDirectory(elasticUrl, repoName)
        if respone == 200:
            responsedata = SnapshotSingleIndex(elasticUrl, repoName, snapshotName, indexName)
            if responsedata and isRemote:
                CopySnapshotToRemote(elasticBackupPath, remoteHost, remotePort, remoteUser, remotePassword, remoteBackupPath)    
            return responsedata
    else:
        respone = RegisterSnapshotDirectory(elasticUrl, repoName)
        if respone == 200:
            responsedata = SnapshotAllIndex(elasticUrl, repoName, snapshotName)
            if responsedata and isRemote:
                CopySnapshotToRemote(elasticBackupPath, remoteHost, remotePort, remoteUser, remotePassword, remoteBackupPath)    
            return responsedata     
    
def ReadBackupFromRemote(remoteHost, remoteUser, remotePassword, remoteBackupPath):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(remoteHost, username=remoteUser, password=remotePassword)

        sftp = ssh.open_sftp()
        with sftp.open(remoteBackupPath, 'r') as remote_file:
            documents = json.load(remote_file)
        sftp.close()
        ssh.close()

        return documents
    except Exception as e:
        return str(e)    

def RegisterSnapshotDirectory(elasticUrl, repoName):
    try:
        payload = {
            "type": "fs",
            "settings": {
                "location": "/mnt/backups", 
            }
        }
        url = f"http://{elasticUrl}/_snapshot/{repoName}"
        print(url)
        respone = requests.post(url, json=payload)
        print(respone.text)
        return respone.status_code
    except Exception as e:
        print(f"Error: {e}")
        return False

def CopySnapshotToRemote(elasticBackupPath, remoteHost, remotePort, remoteUser, remotePassword, remoteBackupPath):
    try:
        ssh_client = CreateSshClient(remoteHost, remotePort, remoteUser, remotePassword)
        if ssh_client is None:
            return False
        
        remotebackupDir = f'{remoteBackupPath}/{int(datetime.datetime.now().timestamp())}_Elastic_Backup'
        stdin, stdout, stderr = ssh_client.exec_command(f"mkdir -p {remotebackupDir}")
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            print(f"Remote directory {remoteBackupPath} ensured to exist.")
        else:
            print(f"Failed to create remote directory: {stderr.read().decode()}")
            return False
        
        with SCPClient(ssh_client.get_transport()) as scp:
            scp.put(elasticBackupPath, remotebackupDir, recursive=True)
        
        ssh_client.close()
        print(f"Backup copied to remote server: {remoteHost}:{remoteBackupPath}")
        return True
    except Exception as e:
        print(f"Error while copying backup to remote server: {e}")
        return False

def SnapshotSingleIndex(elasticUrl, repoName, snapshotName, indexName):
    try:
        snapshot_payload = {
            "indices": indexName,
            "ignore_unavailable": True,
            "include_global_state": False
        }

        url = f"http://{elasticUrl}/_snapshot/{repoName}/{snapshotName}?wait_for_completion=true"
        response = requests.put(url, json=snapshot_payload)

        # print(response.json())
        return response.json()
    except Exception as e:
        print("Exception occurred while taking snapshot. ",e)
        return False

def SnapshotAllIndex(elasticUrl, repo_name, snapshot_name):
    try:
        all_indices = requests.get(f"http://{elasticUrl}/_cat/indices?v").text.splitlines()

        # Step 2: Filter out indices that start with a dot
        indices_to_snapshot = [index.split()[2] for index in all_indices if not index.startswith('.')]

        snapshot_payload = {
            "indices": ",".join(indices_to_snapshot),
            "ignore_unavailable": True,
            "include_global_state": False
        }

        response = requests.put(
            f"http://{elasticUrl}/_snapshot/{repo_name}/{snapshot_name}?wait_for_completion=true",
            json=snapshot_payload
        )

        print(response.json())
        return response.json()
    except  Exception as e:
        print(e)
        return False    
        
def RestoreSnapshotsFromElasticPath(indexName, elasticUrl, repoName, snapshotName):
    if indexName:
        responsedata = RestoreSingleIndex(elasticUrl, repoName, snapshotName, indexName)
        return responsedata
    else:
        responsedata = RestoreAllIndices(elasticUrl, repoName, snapshotName)
        return responsedata 

def RestoreSingleIndex(elasticUrl, repoName, snapshotName, indexName):
    try:
        restore_payload = {
            "indices": indexName,
            "ignore_unavailable": True,
            "include_global_state": False
        }

        url = f"http://{elasticUrl}/_snapshot/{repoName}/{snapshotName}/_restore"
        response = requests.post(url, json=restore_payload)

        if response.status_code == 200:
            print("Snapshot restore successful for index:", indexName)
        else:
            print(f"Failed to restore snapshot: {response.status_code} - {response.text}")

        return response.json()
    except Exception as e:
        print("Exception occurred while restoring snapshot. ", e)
        return False

def RestoreAllIndices(elasticUrl, repoName, snapshotName):
    try:
        restore_payload = {
            "ignore_unavailable": "true",
            "include_global_state": "false"
        }

        url = f"http://{elasticUrl}/_snapshot/{repoName}/{snapshotName}/_restore"
        response = requests.post(url, json=restore_payload)

        if response.status_code == 200:
            print("Snapshot restore successful for all indices.")
        else:
            print(f"Failed to restore snapshot: {response.status_code} - {response.text}")

        return response.json()
    except Exception as e:
        print("Exception occurred while restoring snapshot. ", e)
        return False

def CopySnapshotFromRemote(remoteHost, remotePort, remoteUser, remotePassword, remoteBackupPath, localBackupPath="/tmp/backups",localpassword="shyena@123"):
    try:
        ssh_client = CreateSshClient(remoteHost, remotePort, remoteUser, remotePassword)
        if ssh_client is None:
            return False
        
        with SCPClient(ssh_client.get_transport()) as scp:
            scp.get(f"{remoteBackupPath}/.", localBackupPath, recursive=True)
            
        command = f"echo {localpassword} | sudo -S chown -R elasticsearch:elasticsearch {localBackupPath}/*"
        
        subprocess.run(command, shell=True, check=True)
        # ssh_client.exec_command(f"echo {remotePassword} | sudo -S chown elasticsearch:elasticsearch {localBackupPath}/*")
        print(f"Changed ownership of {localBackupPath} to 'elastic'")

        command = f"echo {localpassword} | sudo -S mv {localBackupPath}/* {'/mnt/backups'}"
        subprocess.run(command, shell=True, check=True)
        
        # ssh_client.exec_command(f"echo {remotePassword} | sudo -S mv {localBackupPath}/* {'/mnt/backups'}")
        print(f"Moved files from {localBackupPath} to {'/mnt/backups'}")
        
        # ssh_client.exec_command(f"echo {remotePassword} | sudo -S rm -rf {localBackupPath}/")
        subprocess.run(f"echo {localpassword} | sudo -S rm -rf {localBackupPath}/", shell=True, check=True)
        print(f"Deleted temporary directory: {localBackupPath}")

        ssh_client.close()
        print(f"Snapshot copied from remote server: {remoteHost}:{remoteBackupPath} to local: {localBackupPath}")
        return True
    except Exception as e:
        print(f"Error while copying snapshot from remote server: {e}")
        return False

def ListAvailableSnapshots(elasticUrl, repoName=None, snapshotName=None):
    snapshotName = snapshotName if snapshotName else '_all'
    if snapshotName and repoName:
        url = f'http://{elasticUrl}/_snapshot/{repoName}/{snapshotName}'
        response = requests.get(url)
        
        return response.json()
    else:
        url = f'http://{elasticUrl}/_cat/repositories?v=true&format=json'
        response = requests.get(url)
        
        return response.json()

def FormatSize(sizeInBytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if sizeInBytes < 1024.0:
            return f"{sizeInBytes:.2f} {unit}"
        sizeInBytes /= 1024.0
    return f"{sizeInBytes:.2f} TB"

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
            