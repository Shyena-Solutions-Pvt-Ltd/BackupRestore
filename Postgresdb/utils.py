import os
import subprocess
import  datetime
from .views import *
import re
import paramiko


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

# Create dir if not exist for remote use
def CreateRemoteDirectoryIfNotExists(sftp, path):
    try:
        sftp.mkdir(path)
    except IOError as e:
        # Directory already exists or another IOError
        print(f"Directory {path} already exists or cannot be created: {str(e)}")

# Server backup for local and remote
def ServerSchemaBackup(user, host, port, password, filePath, localPath, isRemote=False, remoteHost=None, remoteUser=None, remotePassword=None):
    os.environ['PGPASSWORD'] = password
    # remote_backup_filepath = None
    if not isRemote:
        if not os.path.exists(localPath):
            os.makedirs(localPath)
        localPath = os.path.join(localPath, f'{int(datetime.datetime.now().timestamp())}_{host}_schema.sql')
        temp_filepath = localPath + ".tmp"
        command = [
                'pg_dumpall',
                '-U', user,
                '-h', str(host),
                '-p', str(port),
                '--schema-only',
                '-v', 
                '-f',temp_filepath
                ]
        result = subprocess.run(command,check=True)
        
        with open(temp_filepath, 'r') as infile, open(localPath, 'w') as outfile:
            for line in infile:
                if 'CREATE ROLE postgres' in line or 'ALTER ROLE postgres' in line:
                    continue  # Skip lines related to postgres role
                outfile.write(line)

        os.remove(temp_filepath)
        
        if result.returncode != 0:
            print(f"Backup failed: {result.stderr.decode()}")
            return False
        else:
            print(f"Backup successfull. File saved to {localPath}")
            return localPath

    else:
        try:
            # Set up SSH connection to remote host
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=remoteHost, username=remoteUser, password=remotePassword)
            print("SSH connection established.")
                
            sftp = ssh.open_sftp()
            print("Attempting to create remote directory...")
            CreateRemoteDirectoryIfNotExists(sftp, filePath)
            print("Remote directory created.")

            remote_backup_filepath = f"{filePath}/{int(datetime.datetime.now().timestamp())}_{host}_schema.sql"
            
            command = f"PGPASSWORD={password} pg_dumpall -U {user} -h {host} -p {port} --schema-only -v"
            print(f"Executing command: {command}")
            
            stdin, stdout, stderr = ssh.exec_command(command)
            print("Backup command executed.")
            
            # with ssh.open_sftp().file(remote_backup_filepath, 'w') as remote_file:
            with sftp.file(remote_backup_filepath, 'w') as remote_file:
                print("Transferring and filtering backup file...")
                
                for line in iter(stdout.readline, ""):
                    line_stripped = line.strip()
                    print(line_stripped) 
                    if 'CREATE ROLE postgres' in line_stripped or 'ALTER ROLE postgres' in line_stripped:
                        continue
                    remote_file.write(line_stripped + '\n')
            
            error_output = stderr.read().decode()
            if error_output:
                print(f"Backup failed with error: {error_output}")
                return remote_backup_filepath

            print(f"Backup saved to remote server at: {remote_backup_filepath}")
            return remote_backup_filepath

        except Exception as e:
            print(f"Error during backup: {str(e)}")
            return remote_backup_filepath

        finally:
            if 'PGPASSWORD' in os.environ:
                del os.environ['PGPASSWORD']
            ssh.close()
def ServerDataBackup( user, host, port, password, filePath, localPath, isRemote=False, remoteHost=None, remoteUser=None, remotePassword=None):
    os.environ['PGPASSWORD'] = password

    if not isRemote:
        # Local backup
        try:
            if not os.path.exists(localPath):
                os.makedirs(localPath)
            # Execute the command locally and save the output to the backup file
            backupFilePath = os.path.join(localPath, f'{int(datetime.datetime.now().timestamp())}_{host}_data_backup.sql')
            command = f"pg_dumpall -U {user} -h {host} -p {port} | grep -v 'CREATE ROLE postgres' | grep -v 'ALTER ROLE postgres' > {backupFilePath}"
            # command = f"pg_dumpall -U {user} -h {host} -p {port} -f {backupFilePath}"
            
            with open(backupFilePath, 'w') as backup_file:
                result = subprocess.run(command, shell=True, stdout=backup_file, stderr=subprocess.PIPE)

            # Check if the command succeeded
            if result.returncode != 0:
                print(f"Backup failed: {result.stderr.decode()}")
                return False
            else:
                print(f"Backup successful. File saved to {backupFilePath}")
                return backupFilePath

        except subprocess.CalledProcessError as e:
            print(f"Backup failed: {e}")
            return False
        
    else:
        # Remote backup using SSH
        try:
            # Connect to the remote server
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=remoteHost, username=remoteUser, password=remotePassword)
                print("SSH connection established.")
            except Exception as e:
                print(f"SSH connection error: {e}")
                return False

            remote_backup_filepath = f"{filePath}/{int(datetime.datetime.now().timestamp())}_{host}_data_backup.sql"
            
            # Command to run pg_dumpall for full server backup
            command = f"PGPASSWORD={password} pg_dumpall -U {user} -h {host} -p {port} -v"
            print(f"Executing command: {command}")
            
            # Execute the command on the remote host to fetch the full server backup
            stdin, stdout, stderr = ssh.exec_command(command)
            sftp = ssh.open_sftp()
            
            with sftp.file(remote_backup_filepath, 'w') as remote_file:
                print("Transferring and filtering backup file...")
                
                for line in iter(stdout.readline, ""):
                    line_stripped = line.strip()
                    print(line_stripped) 
                    if 'CREATE ROLE postgres' in line_stripped or 'ALTER ROLE postgres' in line_stripped:
                        continue
                    remote_file.write(line_stripped + '\n')
            
            print(f"Full server backup saved to remote server at: {remote_backup_filepath}")
            return remote_backup_filepath
        
        except Exception as e:
            print(f"Error during remote backup: {e}")
            return None
        
        finally:
            if 'PGPASSWORD' in os.environ:  # Ensure the environment variable is cleared
                del os.environ['PGPASSWORD']
            if ssh:
                ssh.close()

# Server restore for local
def ServerSchemaRestore(user, host, port, password, filePath):
    db_names = set()
    with open(filePath, 'r') as file:
        content = file.read()
        db_names = set(re.findall(r'CREATE\s+DATABASE\s+("([^"]+)"|([^\s]+))\s+WITH\s+', content, re.IGNORECASE))
        db_names = {match[1] if match[1] else match[2] for match in db_names}
        print("Database Names: ",db_names)
    
    for dbName in db_names:
        command = f'CREATE DATABASE \"{dbName}\";'
        print(command)
        os.environ['PGPASSWORD'] = password
        try:
            result = subprocess.run(
                ['psql', '-U', user, '-h', host, '-p', str(port), '-c', command],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if result.returncode == 0:
                print(f"Database restored successfully from {filePath}")
            else:
                print(f"Restoration failed")
                print(f"Error Output: {result.stderr.decode()}")
                return False
        except subprocess.CalledProcessError as e:
            print(f"Restore failed: {e}")
            return str(e)
    return filePath
def ServerDataRestore( user, host, port, password, filePath):
    print(filePath)
    os.environ['PGPASSWORD'] = password
    command = [
        'psql',
        '-U', user,
        '-h', host,
        '-p', str(port),
        '-f', filePath
    ]
    try:
        # Run the command
        result = subprocess.run(command, stderr=subprocess.PIPE, check=True)
        
        # Check if the command was successful
        if result.returncode == 0:
            print(f"Server restored successfully from {filePath}")
            return filePath
        else:
            print(f"Restoration failed")
            print(f"Error Output: {result.stderr.decode()}")
            return False
    except subprocess.CalledProcessError as e:
        print(f"Restore failed: {e}")
        return False
    finally:
        del os.environ['PGPASSWORD']

# Server restore for remote use
def RestoreServerFromRemote(remote_host, remote_user, remote_password, local_host, db_user, db_port, db_password, schema_file_path, data_file_path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Connect to the remote server
        ssh.connect(remote_host, username=remote_user, password=remote_password)
        stdin, stdout, stderr = ssh.exec_command(f'cat {schema_file_path}')
        content = stdout.read().decode('utf-8')

        # Extract database names from the schema content
        db_names = set(re.findall(r'CREATE\s+DATABASE\s+("([^"]+)"|([^\s]+))\s+WITH\s+', content, re.IGNORECASE))
        db_names = {match[1] if match[1] else match[2] for match in db_names}
        print("Database Names: ", db_names)

        for dbName in db_names:
            # Create the database on the local PostgreSQL instance
            command = f'CREATE DATABASE "{dbName}";'
            print(command)
            os.environ['PGPASSWORD'] = db_password
            try:
                result = subprocess.run(
                    ['psql', '-U', db_user, '-h', local_host, '-p', str(db_port), '-c', command],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                if result.returncode == 0:
                    print(f"Database '{dbName}' created successfully.")
                else:
                    print(f"Failed to create database '{dbName}'")
                    print(f"Error Output: {result.stderr}")
                    return False

                restore_command = f'PGPASSWORD={db_password} psql -U {db_user} -h {local_host} -p {db_port} -d "{dbName}" -f {schema_file_path}'

                # Restore tables from the schema file for the current database
                restore_stdin, restore_stdout, restore_stderr = ssh.exec_command(restore_command)
                
                while True:
                    # Flush stdout and stderr in real-time
                    output = restore_stdout.readline()
                    if output == '' and restore_stdout.channel.exit_status_ready():
                        break
                    if output:
                        print(output.strip())
                
                # Read the output and error
                restore_error = restore_stderr.read().decode('utf-8')
                if restore_error:
                    print(f"Failed to restore tables for database '{dbName}': {restore_error}")
                else:
                    print(f"Tables restored successfully for database '{dbName}' from remote schema file.")
                
                data_restore_command = f'PGPASSWORD={db_password} psql -U {db_user} -h {local_host} -p {db_port} -d "{dbName}" -f {data_file_path}'
                data_restore_stdin, data_restore_stdout, data_restore_stderr = ssh.exec_command(data_restore_command)

                while True:
                    # Flush stdout and stderr in real-time for data restoration
                    data_output = data_restore_stdout.readline()
                    if data_output == '' and data_restore_stdout.channel.exit_status_ready():
                        break
                    if data_output:
                        print(data_output.strip())

                # Read the data restoration error
                data_restore_error = data_restore_stderr.read().decode('utf-8')
                if data_restore_error:
                    print(f"Failed to restore data for database '{dbName}': {data_restore_error}")
                else:
                    print(f"Data restored successfully for database '{dbName}' from remote data file.")


            except subprocess.CalledProcessError as e:
                print(f"Database creation/restore failed: {e}")
                # return str(e)

    except Exception as e:
        print(f"SSH connection failed: {e}")
    finally:
        # Ensure the SSH connection is closed
        ssh.close()

    return True


#Local Case Backup
def LocalCaseQuery(startTime, endTime, user, host, port, password, dbname, filePath):
    os.environ["PGPASSWORD"] = password
    
    if not os.path.exists(filePath):
        os.makedirs(filePath)
    
    caseBackupDir = os.path.join(filePath, 'case')
    if not os.path.exists(caseBackupDir):
        os.makedirs(caseBackupDir)
    
    schemabackupFilePath = os.path.join(filePath, f'{dbname}_schema_backup_{datetime.datetime.now().strftime("%d%m%Y")}.sql')
    # pg_dump command to create a schema-only backup
    command = [
        'pg_dump',
        '-h', str(host),
        '-p', str(port),
        '-U', user,
        '-d', dbname,
        '--schema-only',  # Option to backup only the schema
        '-v', 
        '-f', schemabackupFilePath
    ]
    
    # Run the backup command
    subprocess.run(command, check=True)
    print(f"Schema backup successful! for database {dbname}. Saved to: {schemabackupFilePath}")

    # Define the export queries and output file paths
    queries = [
    {
            "query": f"COPY (SELECT * FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}') TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir, "Case_Management_case.csv")
    },
    {
            "query": f"COPY (SELECT * from public.\"Case_Management_job\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_job.csv")
    },
    {
            "query": f"COPY (SELECT * from public.\"Case_Management_caseusermappingtable\" WHERE case_id_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_caseusermappingtable.csv")
    },
    {
            "query": f"COPY (SELECT * from public.\"Case_Management_job_target\" WHERE job_id IN (SELECT job_id from public.\"Case_Management_job\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}'))) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_job_target.csv")
    },
    {
            "query": f"COPY (SELECT * from public.\"Case_Management_job_target_group\" WHERE job_id IN (SELECT job_id from public.\"Case_Management_job\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}'))) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_job_target_group.csv")
    },
    {
            "query": f"COPY (SELECT * from public.\"Case_Management_case_target\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_case_target.csv")
    },
    {
            "query": f"COPY (SELECT * from public.\"Case_Management_case_target_group\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_case_target_group.csv")
    },
    {
            "query": f"COPY (SELECT * from public.\"Case_Management_useruploadtable_case\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_useruploadtable_case.csv")
    }
    ]

    return queries
def RunPsql(query, output_file, user, host, port, dbname):
    try:
        command = f"psql -U {user} -h {host} -p {port} -d {dbname} -c \'{query}\' > {output_file}"
        print(f"Running command: {command}")
        subprocess.run(command, shell=True, check=True)
        print(f"Data exported to {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}\n{e}")

#Local Case Restore
def ExtractTableNames(SCHEMA_FILE_PATH):
    with open(SCHEMA_FILE_PATH, 'r') as schema_file:
        schema_sql = schema_file.read()
    
    table_names = re.findall(r'CREATE TABLE\s+(?:\w+\.)?"?([a-zA-Z_][a-zA-Z0-9_]*)"?', schema_sql)
    return table_names
def RestoreCaseQueryData(user, host, port, dbname, password, tableName, filePath, schemaPath):
    os.environ['PGPASSWORD'] = password
    
    restore_command = [
        'psql',
        '-U', user,
        '-h', host,
        '-p', str(port),
        '-d', dbname,
        '-f', schemaPath
    ]
    try:
        result = subprocess.run(restore_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Schema restored successfully to database '{dbname}'.")
        print(f"Output: {result.stdout.decode()}")
    except subprocess.CalledProcessError as e:
        print(f"Schema restoration failed for '{dbname}': {e}")
        print(f"Error Output: {e.stderr.decode()}")

    # Execute the COPY command using subprocess
    command = [
        'psql',
        '-U', user,
        '-h', host,
        '-p', str(port),
        '-d', dbname,
        '-c', f"\COPY \"{tableName}\" FROM '{filePath}' WITH (FORMAT csv, HEADER true)"
    ]

    try:
        subprocess.run(command, check=True, text=True, capture_output=True)
        print(f"Successfully restored table {tableName} from {filePath}.")
    except subprocess.CalledProcessError as e:
        print(f"Error restoring table {tableName} from {filePath}:")
        print(e.stderr)
    except Exception as e:
        print(f"Exception occurred: {e}")
    finally:
        os.environ.pop("PGPASSWORD", None)

#Remote Case Backup
def BackupCaseQueryRemote(startTime, endTime, user, host, port, password, dbname, filePath, remote_host, remote_user, remote_password):
    schemabackupFilePath = os.path.join(filePath, f'{dbname}_schema_backup_{int(datetime.datetime.now().timestamp())}.sql')

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=remote_host, username=remote_user, password=remote_password)
    
    sftp = ssh.open_sftp()
    print("Attempting to create remote directory...")
    CreateRemoteDirectoryIfNotExists(sftp, filePath)
    caseBackupDir = os.path.join(filePath, 'case')
    CreateRemoteDirectoryIfNotExists(sftp, caseBackupDir)
    
    print("Remote directory created.")
    
    command = f"PGPASSWORD={password} pg_dump -U {user} -h {host} -p {port} -d {dbname} --schema-only -v > {schemabackupFilePath}"

    stdin, stdout, stderr = ssh.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()
    if exit_status == 0:
        print(f"Schema backup successful! for database {dbname}. Saved to: {schemabackupFilePath}")
    else:
        print(f"Error during schema backup: {stderr.read().decode()}")

    
    # Define the export queries and output file paths
    queries = [
        {
            "query": f"COPY (SELECT * FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}') TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir, "Case_Management_case.csv")
        },
        {
            "query": f"COPY (SELECT * from public.\"Case_Management_job\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_job.csv")
        },
        {
            "query": f"COPY (SELECT * from public.\"Case_Management_caseusermappingtable\" WHERE case_id_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_caseusermappingtable.csv")
        },
        {
            "query": f"COPY (SELECT * from public.\"Case_Management_job_target\" WHERE job_id IN (SELECT job_id from public.\"Case_Management_job\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}'))) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_job_target.csv")
        },
        {
            "query": f"COPY (SELECT * from public.\"Case_Management_job_target_group\" WHERE job_id IN (SELECT job_id from public.\"Case_Management_job\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}'))) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_job_target_group.csv")
        },
        {
            "query": f"COPY (SELECT * from public.\"Case_Management_case_target\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_case_target.csv")
        },
        {
            "query": f"COPY (SELECT * from public.\"Case_Management_case_target_group\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_case_target_group.csv")
        },
        {
            "query": f"COPY (SELECT * from public.\"Case_Management_useruploadtable_case\" WHERE case_id IN (SELECT id FROM public.\"Case_Management_case\" WHERE updated_on >= '{startTime}' and updated_on <= '{endTime}')) TO STDOUT WITH CSV HEADER;",
            "output_file": os.path.join(caseBackupDir,"Case_Management_useruploadtable_case.csv")
        }
    ]

    # return queries
    for query in queries:
        output_file = query["output_file"]
        query_str = query["query"]
        
        # Command to execute the COPY query
        copy_command = f"PGPASSWORD={password} psql -U {user} -h {host} -p {port} -d {dbname} -c \'{query_str}\' > {output_file}"
        
        # Execute the command
        stdin, stdout, stderr = ssh.exec_command(copy_command)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            print(f"Data exported successfully to {output_file}.")
        else:
            print(f"Error exporting data to {output_file}: {stderr.read().decode()}")

    # Close the SSH connection
    ssh.close()
    return [{"status": True, "message": "All operations completed successfully."}]

#Remote Case Restore
def ExtractTableNamesFromRemote(remote_host, remote_user, remote_password, schema_file_path):
    try:
        # Create an SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(remote_host, username=remote_user, password=remote_password)

        # Use SFTP to retrieve the schema file
        sftp = ssh.open_sftp()
        with sftp.open(schema_file_path, 'r') as schema_file:
            schema_sql = schema_file.read()

        schema_sql = schema_sql.decode('utf-8')
        
        # Regular expression to match CREATE TABLE statements
        table_names = re.findall(r'CREATE TABLE\s+(?:\w+\.)?"?([a-zA-Z_][a-zA-Z0-9_]*)"?', schema_sql)

        # Close the SFTP connection
        sftp.close()
        ssh.close()

        return table_names

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
def RestoreCaseQueryFromRemote(remote_host, remote_user, remote_password, local_host, db_user, db_port, db_password, db_name, schema_file_path, data_file_path):
    os.environ['PGPASSWORD'] = db_password

    try:
        table_names = ExtractTableNamesFromRemote(remote_host, remote_user, remote_password, schema_file_path)

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(remote_host, username=remote_user, password=remote_password)

        restore_schema_command = f"PGPASSWORD={db_password} psql -U {db_user} -h {local_host} -d {db_name} -f {schema_file_path}"
        
        stdin, stdout, stderr = ssh.exec_command(restore_schema_command)
        exit_status = stdout.channel.recv_exit_status()  # Wait for command to complete

        for table_name in table_names:
            remote_csv_file_path = os.path.join(data_file_path, f"{table_name}.csv")
            
            check_csv_command = f"ls {remote_csv_file_path}"
            stdin, stdout, stderr = ssh.exec_command(check_csv_command)
            exit_status = stdout.channel.recv_exit_status()

            if exit_status == 0:  # The file exists
                qtablename = f'\\\"{table_name}\\\"'
                copy_command = f"\\COPY {qtablename} FROM '{remote_csv_file_path}' WITH (FORMAT csv, HEADER true)"
                restore_data_command = f"PGPASSWORD={db_password} psql -U {db_user} -h {local_host} -p {db_port} -d {db_name} -c \"{copy_command}\" "
                
                print("Restore Data Command:", restore_data_command)
                stdin, stdout, stderr = ssh.exec_command(restore_data_command)
                exit_status = stdout.channel.recv_exit_status()

                if exit_status == 0:
                    print(f"Successfully restored data from {remote_csv_file_path} into table '{table_name}'.")
                else:
                    error_output = stderr.read().decode()
                    print(f"Error restoring table {table_name} from {remote_csv_file_path}: {error_output}")
                    print(f"Table Name: {table_name}")
                    print(f"Remote CSV File Path: {remote_csv_file_path}")
                    print(f"Copy Command: {copy_command}")
            else:
                print(f"CSV file {remote_csv_file_path} does not exist on the remote machine.")

    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    finally:
        os.environ.pop("PGPASSWORD", None)  # Clean up the environment variable
        if ssh:
            ssh.close()

# Optional use of scheme restore of database
def RestoreSchemaForDatabase(user, host, port, dbname, password, schemaBackupPath):
    os.environ['PGPASSWORD'] = password

    checkDbCommand = [
        'psql',
        '-U', user,
        '-h', host,
        '-p', str(port),
        '-d', 'postgres',  # Connect to default database to check
        '-tAc', f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'"
    ]
    try:
        print(f"Checking if database '{dbname}' exists...")
        result = subprocess.run(checkDbCommand, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        
        # If result stdout is empty, it means the database doesn't exist
        if not result.stdout.strip():
            raise Exception("Database does not exist")
        print(f"Database '{dbname}' exists.")
    except Exception as e:
        print(f"Database '{dbname}' does not exist, creating database...")
        try:
            createDbCommand = f'CREATE DATABASE "{dbname}";'
            subprocess.run(
                ['psql', '-U', user, '-h', host, '-p', str(port), '-d', 'postgres', '-c', createDbCommand],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            print(f"Database '{dbname}' created successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error during database creation: {e.stderr}")
            return False
            
    try:
        # Restore schema
        command = [
            'psql',
            '-U', user,
            '-h', host,
            '-p', str(port),
            '-d', dbname,
            '-f', schemaBackupPath
        ]
        subprocess.run(command, check=True)
        print(f"Schema restoration successful for database: {dbname}")
    except subprocess.CalledProcessError as e:
        print(f"Error during schema restoration: {str(e)}")
        return False
    finally:
        os.environ.pop('PGPASSWORD', None)
    
    return dbname

# CMM only Schema backup
def DatabaseSchemaBackup(user, host, port, password, dbName, filePath):
    os.environ['PGPASSWORD'] = password
    filePath = os.path.join(filePath, f'{datetime.datetime.now().strftime("%d%m%Y")}_{host}_{dbName}_schema.sql')
    tempFilepath = filePath + ".tmp"
    
    command = [
        'pg_dump',
        '-U', user,
        '-h', str(host),
        '-p', str(port),
        '--schema-only',
        '-v', 
        '-f', tempFilepath,
        dbName
    ]
    
    result = subprocess.run(command, check=True)
    
    with open(tempFilepath, 'r') as infile, open(filePath, 'w') as outfile:
        for line in infile:
            if 'CREATE ROLE postgres' in line or 'ALTER ROLE postgres' in line:
                continue  # Skip lines related to postgres role
            outfile.write(line)

    os.remove(tempFilepath)
    
    if result.returncode != 0:
        print(f"Backup failed: {result.stderr.decode()}")
        return False
    else:
        # print(f"Backup successful. File saved to {filePath}")
        return True
    
# Function to backup server schema local
# def ServerSchemaBackup( user, host, port, password, filePath):
#     os.environ['PGPASSWORD'] = password
#     if not os.path.exists(filePath):
#         os.makedirs(filePath)
#     filePath = os.path.join(filePath, f'{datetime.datetime.now().timestamp()}_{host}_schema.sql')
#     temp_filepath = filePath + ".tmp"
#     command = [
#             'pg_dumpall',
#             '-U', user,
#             '-h', str(host),
#             '-p', str(port),
#             '--schema-only',
#             '-v', 
#             '-f',temp_filepath
#             ]
#     result = subprocess.run(command,check=True)
    
#     with open(temp_filepath, 'r') as infile, open(filePath, 'w') as outfile:
#         for line in infile:
#             if 'CREATE ROLE postgres' in line or 'ALTER ROLE postgres' in line:
#                 continue  # Skip lines related to postgres role
#             outfile.write(line)

#     os.remove(temp_filepath)
    
#     if result.returncode != 0:
#         print(f"Backup failed: {result.stderr.decode()}")
#         return False
#     else:
#         # print(f"Backup successfull. File saved to {filePath}")
#         return True

# # Function to backup server data local
# def ServerDataBackup( user, host, port, password, filePath):
#     os.environ['PGPASSWORD'] = password
#     if not os.path.exists(filePath):
#         os.makedirs(filePath)
#     backupFIlePath = os.path.join(filePath, f'{int(datetime.datetime.now().timestamp())}_{host}_backup.sql')
#     command = f"pg_dumpall -U {user} -h {host} -p {port} | grep -v 'CREATE ROLE postgres' | grep -v 'ALTER ROLE postgres' > {backupFIlePath}"

#     try:
#         result = subprocess.run(command, shell=True, stderr=subprocess.PIPE)

#         if result.returncode != 0:
#             print(f"Backup failed: {result.stderr.decode()}")
#             return False
#         else:
#             print(f"Backup successfull. File saved to {backupFIlePath}")
#             return backupFIlePath
            
#     except subprocess.CalledProcessError as e:
#         print(f"Backup failed: {e}")
#     finally:
#         del os.environ['PGPASSWORD']

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


def qqq(session, keyspace, table, dateColumn):
    try:
        query = f"SELECT min({dateColumn}), max({dateColumn}) FROM {keyspace}.{table};"
        result = session.execute(query).one()
        minDate, maxDate = result
        return minDate, maxDate
    except Exception as e:
        print(f"Could not get date range for {keyspace}.{table}: {e}")
        return None, None    
    