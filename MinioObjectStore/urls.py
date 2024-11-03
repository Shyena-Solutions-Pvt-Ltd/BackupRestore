from django.urls import path
from .views import *

urlpatterns = [
    path('ListBuckets/', BucketList.as_view(),name='List-Buckets'),
    path('BackupMinio/', MinioBackup.as_view(),name='Minio-Backup'),
    path('RestoreMinio/', MinioRestore.as_view(),name='Minio-Restore'),
]