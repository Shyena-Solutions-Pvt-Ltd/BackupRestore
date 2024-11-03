from django.urls import path
from .views import *

urlpatterns = [
    path('ListIndexes/', ViewIndexes.as_view(),name='List-Indexes'),
    path('BackupIndexes/',BackupIndexes.as_view(),name='Backup-Indexes'),
    path('RestoreIndexesFromRemote/',RestoreIndexesFromRemote.as_view(),name='Restore-Files'),
    path('SnapshotRepository/',RegisterSnapshotRepository.as_view(),name='Register-Repository'),
    path('RestoreIndexes/',RestoreSnapshots.as_view(),name='Restore-Snapshots'),
]