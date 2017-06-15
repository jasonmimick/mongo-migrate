mongo-migrate
=============

```
$python mongo-migrate.py --help
usage: mongo-migrate.py [-h] --source SOURCE --destination DESTINATION
                        [--oplogOnly] [--initialSyncOnly]
                        [--dropOnDestination] [--loglevel LOGLEVEL]
                        [--logfile LOGFILE]
                        [--numInsertionWorkers NUMINSERTIONWORKERS]
                        [--tailSleepTimeSeconds TAILSLEEPTIMESECONDS]
                        [--collectionCheckSleepCycles COLLECTIONCHECKSLEEPCYCLES]
                        [--initialSyncBatchSize INITIALSYNCBATCHSIZE]

mongo-migrate - replica et synchonization tool

optional arguments:
  -h, --help            show this help message and exit
  --source SOURCE       mongodb uri for source replica set
  --destination DESTINATION
                        mongodb uri for destination replica set
  --oplogOnly           only stream oplog from source to destination
  --initialSyncOnly     only all data from source to destination, do not
                        append oplog entries
  --dropOnDestination   drop collections on destination, default False
  --loglevel LOGLEVEL   loglevel debug,info default=info
  --logfile LOGFILE     logfile full path or -- for stdout
  --numInsertionWorkers NUMINSERTIONWORKERS
                        number of threads run parallel initial sync for
                        collections, default 5
  --tailSleepTimeSeconds TAILSLEEPTIMESECONDS
                        seconds to sleep when source oplog does not have new
                        data, default 5
  --collectionCheckSleepCycles COLLECTIONCHECKSLEEPCYCLES
                        run collection checks after this many oplog tailing
                        sleep cycles, default 12
  --initialSyncBatchSize INITIALSYNCBATCHSIZE
                        Batch size for initial sync, default 1000
```
