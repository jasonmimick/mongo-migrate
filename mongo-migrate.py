#!/user/bin/env python

# mongo-migrate
#
# Live migration of data from source replica set to destination replica set
#
# Usage:
# $python mongo-migrate.py --source <mongodb-uri> --destination <mongodb-uri>
#
#

import datetime
import pymongo
import time
import yaml
import logging
import sys, os
import argparse
from threading import Thread
import traceback
import multiprocessing


class OplogConsumer(multiprocessing.Process):

    def __init__(self, args, logger, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.args = args
        self.logger = logger
        self.destination = args.destination
        self.dest_mongo = get_mongo_connection( self.destination )
        self.daemon = True

    def run(self):
        self.logger.info("Oplog Consumer run()")
        proc_name = self.name
        while True:
            try:
                next_op = self.task_queue.get()
                if next_op is None:
                    # Poison pill means shutdown
                    self.logger.info('%s: Oplog Consumer Exiting' % proc_name)
                    self.task_queue.task_done()
                    break
                if '___.HeartbeatOp.___' in next_op:
                    self.logger.debug('Oplog Consumer got heartbeat at %s' % (next_op['___.HeartbeatOp.___']))
                    continue
                self.logger.debug('%s: %s' % (proc_name, next_op))
                result = self.process_op(next_op)
                self.task_queue.task_done()
                self.result_queue.put(result)
            except (KeyboardInterrupt, SystemExit):
                self.logger.info('%s: Oplog Consumer Exiting - got SIGINT' % proc_name)
                self.task_queue.task_done()
                break
        return

    def process_op(self,op):
        try:
            r = self.dest_mongo['admin'].command('applyOps',[op])
            return r
        except Exception as exp:
            self.logger.error(exp)
            return exp



class App():

    def __init__(self, args, logger):
        self.args = args
        self.source = self.args.source
        self.destination = self.args.destination
        self.logger = logger
        self.stop_requested = False
        self.oplog_tasks = multiprocessing.JoinableQueue()
        self.oplog_tasks_results = multiprocessing.Queue()
        # start oplog_tasks_results monitor
        self.oplog_tasks_results_monitor =Thread(target=self.monitor_oplog_tasks_results, args=(self.oplog_tasks_results,logger))
        self.oplog_tasks_results_monitor.setDaemon(True)
        self.oplog_tasks_results_monitor.start()

        self.oplog_consumer = OplogConsumer(self.args,self.logger,self.oplog_tasks,self.oplog_tasks_results)
        self.oplog_consumer.start()

    def monitor_oplog_tasks_results(self, queue, logger):
        logger.info("Oplog tasks results monitor started")
        while True:
            result = queue.get()
            logger.debug(result)

    def initial_sync(self):
        self.logger.info("initial sync starting")
        source_mongo = get_mongo_connection(self.source)
        dest_mongo = get_mongo_connection(self.destination)

        # fetch and record source last oplog entry
        self.initial_sync_status = {}
        last_oplog_doc = self.get_last_source_oplog_entry()
        self.initial_sync_status['last_source_oplog_entry'] = last_oplog_doc['ts']

        # get list of namespaces to sync
        source_dbs = source_mongo['admin'].command('listDatabases')
        self.logger.debug('source databases: %s' % str(source_dbs))
        # get list of destination namespaces
        dest_dbs = dest_mongo['admin'].command('listDatabases')
        self.logger.debug('destination databases: %s' % str(dest_dbs))
        # check for overlapping collections
        self.logger.info("TODO: check for overlapping collections")

        # dump and restore

        # # start numParallelWorkers threads dumping and restore
        # # each source namespace
        # # wait for each to complete
        numParallelWorkers = self.args.numInsertionWorkers
        threads = []
        dbs_to_skip = [ 'admin', 'local']
        colls_to_skip = [ 'system.indexes' ]
        for database in source_dbs['databases']:
            self.logger.debug('working on source db %s' % str(database))
            if database['name'] in dbs_to_skip:
                self.logger.info('Skipping %s on source' % database['name'])
                continue

            source_colls = source_mongo[database['name']].collection_names()
            self.logger.debug('source collections = %s' % str(source_colls))
            for coll in source_colls:
                if coll in colls_to_skip:
                    self.logger.info('Skipping %s.%s on source' % (database['name'],coll))
                    continue
                self.logger.debug('>>>>>>>>> coll=%s'%coll)
                t = Thread(target=self.dump_and_restore_collection, args=(database['name'],coll))
                t.setDaemon(True)
                threads.append(t)

                self.logger.debug('len(threads)=%i'%len(threads))
                if len(threads)==numParallelWorkers:
                    self.logger.debug('starting batch')
                    [t.start() for t in threads]

                    while True:
                        threads = [t.join(20) for t in threads if t is not None and t.isAlive()]
                    threads = []


            self.logger.debug('len(threads)=%i'%len(threads))
            if len(threads)>0:
                self.logger.debug('starting final batch')
                [t.start() for t in threads]
                wait=True
                while wait:
                    threads = [t.join(20) for t in threads if t is not None and t.isAlive()]
                    self.logger.debug('treads: %s' % str(threads))
                    if len(threads)==0:
                        wait=False

        self.logger.info("Initial sync complete")
        return self.initial_sync_status['last_source_oplog_entry']


    def dump_and_restore_collection(self,db,collection):
        self.logger.info('dump_and_restore %s.%s' % (db,collection))
        source_mongo = get_mongo_connection(self.source)
        dest_mongo = get_mongo_connection(self.destination)
        if self.args.dropOnDestination:
            self.logger.debug('dropping %s.%s on destination' % (db,collection))
            result = dest_mongo[db][collection].drop()
            self.logger.debug('drop result: %s' %str(result))
        # create identical collection on destination
        # TODO: support collection options()
        dest_mongo[db].create_collection(collection)

        source_count = source_mongo[db][collection].count()
        self.logger.info('source_count=%s' % str(source_count))

        doc_count = 0
        bulk = dest_mongo[db][collection].initialize_unordered_bulk_op()
        batch_size = self.args.initialSyncBatchSize
        source_cursor = source_mongo[db][collection].find({},snapshot=True)
        while ( source_cursor.alive ):
            try:
                bulk.insert( source_cursor.next() )
                doc_count += 1
                if (doc_count == batch_size ):
                    r = bulk.execute()
                    self.logger.debug(r)
                    doc_count=0
                    bulk = dest_mongo[db][collection].initialize_unordered_bulk_op()
            except StopIteration:   #thrown when out of data so wait a little
                self.logger.info('%s.%s source intial sync cursor complete' % (db,collection))
                if doc_count>0:
                    r = bulk.execute()
                    doc_count=0
                    self.logger.debug(r)

        # check if any more docs to send
        if doc_count>0:
            r = bulk.execute()
            self.logger.debug(r)

        dest_count = dest_mongo[db][collection].count()
        self.logger.info('dest_count=%s' % str(dest_count))
        # create indexes
        index_query = { "ns" : "%s.%s" % (db,collection) }
        indexes_cursor = source_mongo[db]['system.indexes'].find(index_query)
        while ( indexes_cursor.alive ):
            index = indexes_cursor.next()
            self.logger.info('%s' % str(index))
            if index['name']==u"_id_":
                self.logger.debug("skipping _id index")
                continue
            pyindex = []
            for key in index['key']:
               pi = (key,pymongo.ASCENDING)
               if not index['key'][key]==1.0:
                  pi = (key,pymongo.DESCENDING)
               pyindex.append(pi)
            self.logger.debug(pyindex)
            c = dest_mongo[db][collection]
            options = dict(index)
            options.pop('name',None)
            options.pop('v',None)
            options.pop('ns',None)
            r = c.create_index( pyindex,
                                name=index['name'],
                                background=True,
                                **options)
            self.logger.info('create_index on %s.%s result:%s' % (db,collection,str(r)))



    def get_last_source_oplog_entry(self):
        source_mongo = get_mongo_connection(self.source)
        last_oplog_entry = source_mongo['local']['oplog.rs'].find({}).sort("ts",-1).limit(1).next()
        self.logger.debug('last oplog entry %s' % last_oplog_entry)
        return last_oplog_entry


    def tail_oplog(self,start_ts=0):
        self.logger.info('tail_oplog')
        source_mongo = get_mongo_connection(self.source)
        dest_mongo = get_mongo_connection(self.destination)
        query = {}
        if ( start_ts==0 ):
            try:
                last_oplog_entry = self.get_last_source_oplog_entry()
                query["ts"]={ "$gt" : last_oplog_entry['ts'] }
            except StopIteration:   #thrown when out of data so wait a little
                self.logger.info("local.oplog.rs"+' exists, but no entries found')
        else:
            query['ts']={ "$gt" : start_ts }

        self.logger.debug('tail_oplog starting ts=%s' % str(query['ts']))
        #start tailable cursor
        oplog = source_mongo['local']['oplog.rs'].find(query,tailable=True)
        if 'ts' in query:
            oplog.add_option(8)     # oplogReplay
        sleep_cycles = 0
        while oplog.alive:
            try:
                if self.stop_requested:
                    self.logger.info("Tail for " + local_oplog + " stopping.")
                    oplog.close()
                    break
                else:
                    doc = oplog.next()
                    self.logger.info(doc)
                self.oplog_tasks.put(doc)
                #self.try_insert(sinkConnection,sinkNS,doc)
            except StopIteration:   #thrown when out of data so wait a little
                self.logger.debug("sleep %i" % self.args.tailSleepTimeSeconds)
                self.oplog_tasks.put( { "___.HeartbeatOp.___" : (datetime.datetime.now()) })
                time.sleep(self.args.tailSleepTimeSeconds)
                sleep_cycles += 1
                if (sleep_cycles % self.args.collectionCheckSleepCycles)==0:
                    self.run_collection_checks()

    def run_collection_checks(self):
        self.logger.info('Running collection checks')
        source_mongo = get_mongo_connection(self.source)
        dest_mongo = get_mongo_connection(self.destination)
        source_dbs = source_mongo['admin'].command('listDatabases')
        dest_dbs = dest_mongo['admin'].command('listDatabases')
        dbs_to_skip = [ 'admin', 'local']
        colls_to_skip = [ 'system.indexes' ]
        for database in source_dbs['databases']:
            db = database['name']
            if db in dbs_to_skip:
                continue
            source_colls = source_mongo[db].collection_names()
            for coll in source_colls:
                if coll in colls_to_skip:
                    continue
                source_count = source_mongo[db][coll].count()
                dest_count = dest_mongo[db][coll].count()
                ok = ''
                if (source_count==dest_count):
                    ok = 'Looks good!'
                self.logger.info('%s.%s.count() source=%i, destination=%i %s'
                                 % (db,coll,source_count,dest_count,ok))
                if not (source_count==dest_count):
                    self.logger.error('%s.%s counts did not match!' % (db,coll))


def get_mongo_connection(uri):
    return pymongo.MongoClient(uri)



def main():

    # parse arguments
    description = u'mongo-migrate - replica et synchonization tool'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--source",required=True,help='mongodb uri for source replica set')
    parser.add_argument("--destination",required=True,help='mongodb uri for destination replica set')
    parser.add_argument("--oplogOnly",action='store_true',default=False,help='only stream oplog from source to destination')
    parser.add_argument("--initialSyncOnly",action='store_true',default=False,help='only all data from source to destination, do not append oplog entries')
    parser.add_argument("--dropOnDestination",action='store_true',default=False,help='drop collections on destination, default False')
    parser.add_argument("--loglevel",default='info',help='loglevel debug,info default=info')
    parser.add_argument("--logfile",default='--',help='logfile full path or -- for stdout')
    parser.add_argument("--numInsertionWorkers",type=int,default=5,help='number of threads run parallel initial sync for collections, default 5')
    parser.add_argument("--tailSleepTimeSeconds",default=5,type=int,help='seconds to sleep when source oplog does not have new data, default 5')
    parser.add_argument("--collectionCheckSleepCycles",default=12,type=int,help='run collection checks after this many oplog tailing sleep cycles, default 12')
    parser.add_argument("--initialSyncBatchSize",default=1000,type=int,help='Batch size for initial sync, default 1000')
    args = parser.parse_args()
    logger = logging.getLogger("mongo-migrate")
    logger.setLevel(getattr(logging,args.loglevel.upper()))
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    if args.logfile == '--':
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(os.path.abspath(args.logfile))
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(description)
    logger.info('mongrate startup')
    logger.debug("args: " + str(args))
    logger.info("log level set to " + logging.getLevelName(logger.getEffectiveLevel()))
    app = App(args, logger)
    logger.info('mongo-migrate initialized')
    try:
        logger.info('running...')
        if app.args.oplogOnly:
            #app.tail_oplog(app.get_last_source_oplog_entry()['ts'])
            app.tail_oplog(0)
        else:
            last_op_ts = app.initial_sync()
            logger.info('initial sync complete, starting oplog tailing ts=%s' % last_op_ts)
            if not app.args.initialSyncOnly:
                app.tail_oplog(last_op_ts)
            else:
                logger.info('--initialSyncOnly was true, not appending oplog')

        logger.info('mongo-migrate done.')
        os._exit(0)
    except Exception as exp:
        logger.error(exp)
        logger.debug("got exception going to call sys.exit(1)")
        traceback.print_exc()
        os._exit(1)





if __name__ == '__main__':
    main()
