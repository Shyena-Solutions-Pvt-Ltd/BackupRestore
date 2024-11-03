from django.urls import path
from .views import *

urlpatterns = [
    path('BackupScylla/', ScyllaBackupForSingleTable.as_view(),name='Scylla-Backup'),
    path('CheckScyllaKeyspaceAndTable/',ScyllaKeyspaceAndTable.as_view(), name='Check-Scylla-KeyspaceAndTable'),
    path('RestoreScylla/',ScyllaRestoreForSingleTable.as_view(),name='Scylla-Restore'),
    path('BackupKeyspace/',ScyllaBackupKeyspace.as_view(),name="Backup-Keyspace"),
    path('RestoreKeyspace/',ScyllaRestoreKeyspace.as_view(),name="Restore-Keyspace"),
    
    ## Truncation and Deletion
    path('ScyllaTruncate/',ScyllaTruncate.as_view(),name="ScyllaTruncate")
]