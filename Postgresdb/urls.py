from django.urls import path
from .views import *

urlpatterns = [
    path('BackupPostgres/', PostgresBackup.as_view(),name='Postgres-Backup'),
    path('RestorePostgres/',PostgresRestoreServer.as_view(),name='Postgres-Restore'),
    path('CmmRestore/',CaseMMRestoreSchemaWithData.as_view(), name='Schema-Restore'),
]