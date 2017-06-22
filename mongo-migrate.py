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
import urllib
import pymongo
from pymongo.errors import BulkWriteError
from pymongo.cursor import CursorType
import time
import logging
import sys, os
import hashlib
import argparse
import traceback
import threading
from threading import Thread
import multiprocessing
import pickle
from bson.json_util import dumps, loads

OPLOG_TOMBSTONE_FILE = "mongo-migrate-tombstone"

class OplogConsumer(multiprocessing.Process):

    def __init__(self, args, logger, task_queue, result_queue, dest_mongo):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.args = args
        self.logger = logger
        self.destination = args.destination
        self.dest_mongo = dest_mongo
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
            except Exception as exp:
                raise exp
        return

    def process_op(self,op):
        try:
            r = self.dest_mongo['admin'].command('applyOps',[op])
            self.logger.debug("writing tombstone=%s to %s" % (str(tombstone),OPLOG_TOMBSTONE_FILE))
            tombstone = get_tombstone(op,self.args)
            update_tombstone(tombstone,self.logger)
            return r
        except Exception as exp:
            self.logger.error(exp)
            raise exp





class App():

    def __init__(self, args, logger):
        self.args = args
        self.source = self.args.source
        self.destination = self.args.destination
        self.logger = logger
        self.stop_requested = False
        self.oplog_tasks = multiprocessing.JoinableQueue()
        self.oplog_tasks_results = multiprocessing.Queue()
        # remove any empty string
        self.nsToMigrate = filter(None,self.args.nsToMigrate.split(','))
        self.source_mongo = get_mongo_connection(self.source)
        self.dest_mongo = get_mongo_connection(self.destination)
        self.initial_sync_initial_tombstone_not_written = True

    def monitor_oplog_tasks_results(self, queue, logger):
        logger.info("Oplog tasks results monitor started")
        while True:
            result = queue.get()
            logger.debug(result)

    def validate_oplog_tombstone_return_last_ts(self,tombstone):
        if not 'source' in tombstone:
            raise Exception("Invalid tombstone - 'source' missing")
        if not 'last_op' in tombstone:
            raise Exception("Invalid tombstone = 'last_op' missing")
        if not 'ts' in tombstone['last_op']:
            raise Exception("Invalid tombstone = 'last_op' missing")

        if not tombstone['source'] == self.args.source:
            raise Exception("Tombstone has source connection string '%s', but mongo-migrate running against '%s'" % ( tombstone['source'], self.args.source))
        if tombstone['v'] == tool_version():
            ts=tombstone['last_op']['ts']
        elif self.args.skipTombstoneVersionCheck:
            self.logger.warn("Expected tool version %s but found %s, but allowed since --skipTombstoneVersionCheck was True" % (tool_version(), tombstone['v']))
            ts=tombstone['last_op']['ts']
        else:
            self.logger.error("Found tombstone, but versions do not match. Expected %s but found %s" % (tool_version(), tombstone['v']))
            self.logger.error("Check version of mongo-migrate or remove file '%s'" % OPLOG_TOMBSTONE_FILE)
            raise Exception("Tool version in tombstone was not correct")

        return ts

    def check_oplog_tombstone(self):
        if os.path.exists(OPLOG_TOMBSTONE_FILE):
            try:
                with open(OPLOG_TOMBSTONE_FILE) as tsf:
                    raw = tsf.readline()
                    tombstone = loads(raw)
                    self.logger.info("Found and loaded tombstone=%s" % str(tombstone))
                    return self.validate_oplog_tombstone_return_last_ts(tombstone)
            except ValueError as ve:
                self.logger.error(ve)
                self.logger.error("tombstone was not correct format")
                if not self.args.skipTombstoneVersionCheck:
                    raise ve
                else:
                    self.logger.info("tombstone format allowed since --skipTombstoneVersionCheck was true")
                    return 0
            except Exception as exp:
                self.logger.error(exp)
                traceback.print_exc()
                raise exp
        else:
            self.logger.info("tombstone not found, will begin oplog sync from now")
            return 0

    def should_migrate_ns(self,db,coll):
        self.logger.debug("self.nsToMigrate = %s" % self.nsToMigrate)
        if len(self.nsToMigrate)==0:
            return True
        ns = "%s.%s" % (db,coll)
        should_migrate = False
        if ns in self.nsToMigrate:
            should_migrate = True
        self.logger.debug("should_migrate_ns checking %s.%s nsToMigrate=%s return %s"
                          % (db,coll,str(self.nsToMigrate),str(should_migrate)))
        return should_migrate

    def initial_sync(self):
        self.logger.info("initial sync starting")

        # fetch and record source last oplog entry
        self.initial_sync_status = {}
        last_oplog_doc = self.get_last_source_oplog_entry()
        self.initial_sync_status['last_source_oplog_entry'] = last_oplog_doc['ts']

        # get list of namespaces to sync
        source_dbs = self.source_mongo['admin'].command('listDatabases')
        self.logger.debug('source databases: %s' % str(source_dbs))
        # get list of destination namespaces
        dest_dbs = self.dest_mongo['admin'].command('listDatabases')
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
        number_source_dbs = len(source_dbs['databases']) - len(dbs_to_skip)
        db_count = 0
        for database in source_dbs['databases']:
            self.logger.debug('working on source db %s' % str(database))
            if database['name'] in dbs_to_skip:
                self.logger.info('Skipping %s on source' % database['name'])
                continue

            source_colls = self.source_mongo[database['name']].collection_names()
            number_source_colls = len(source_colls) - len(colls_to_skip)
            if number_source_colls<0:
                number_source_colls = 0
            coll_count = 0
            self.logger.debug('source collections = %s' % str(source_colls))
            for coll in source_colls:
                if coll in colls_to_skip:
                    self.logger.info('Skipping %s.%s on source' % (database['name'],coll))
                    continue
                if not self.should_migrate_ns(database['name'],coll):
                    continue
                self.logger.debug('>>>>>>>>> coll=%s'%coll)
                t = Thread(target=self.dump_and_restore_collection, args=(database['name'],coll,self.source_mongo,self.dest_mongo))
                t.setDaemon(True)
                threads.append(t)

                self.logger.debug('len(threads)=%i threads=%s'%(len(threads),threads))
                if len(threads)==numParallelWorkers:
                    self.logger.debug('starting batch')
                    for t in threads:
                        t.start()
                    for t in threads:
                        t.join()
                    self.logger.debug('DONE >>> len(threads)=%i'%len(threads))
                    self.logger.debug("threads=%s" % threads)
                    coll_count += numParallelWorkers;
                    threads = []

                self.logger.info("Status %d out of %d databases complete" % (db_count,number_source_dbs))
                self.logger.info("Status %d out of %d collections for db=%s complete" % (coll_count,number_source_colls,database['name']))

            self.logger.debug('len(threads)=%i'%len(threads))
            if len(threads)>0:
                lastBatchWorkers = len(threads)
                self.logger.debug('starting final batch of %i workers' % lastBatchWorkers)
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                self.logger.debug("last batch len(threads)=%i complete" % len(threads))
                coll_count += lastBatchWorkers;
                threads=[]
            db_count +=1
            self.logger.info("Status %d out of %d databases complete" % (db_count,number_source_dbs))
            self.logger.info("Status %d out of %d collections for db=%s complete" % (coll_count,number_source_colls,database['name']))

        self.logger.info("Initial sync complete")
        return self.initial_sync_status['last_source_oplog_entry']


    def dump_and_restore_collection(self,db,collection,source_mongo,dest_mongo):
        self.logger.info('dump_and_restore %s.%s' % (db,collection))
        source_count = source_mongo[db][collection].count()
        self.logger.info('%s.%s source_count=%s' % (db,collection,str(source_count)))
        if (source_count==0) and not self.args.migrateEmptyCollections:
            self.logger.info("Skipping %s.%s since --migrateEmptyCollection was False" % (db,collection))
            return

        if self.args.dropOnDestination:
            self.logger.debug('dropping %s.%s on destination' % (db,collection))
            result = dest_mongo[db][collection].drop()
            self.logger.debug('drop on %s.%s result: %s' %(db,collection,str(result)))
        # create identical collection on destination
        # TODO: support collection options()
        dest_mongo[db].create_collection(collection)

        doc_count = 0
        bulk = dest_mongo[db][collection].initialize_unordered_bulk_op()
        batch_size = self.args.initialSyncBatchSize
        source_cursor = source_mongo[db][collection].find({},modifiers={"$snapshot":True})
        while ( source_cursor.alive ):
            try:
                doc = source_cursor.next()
                try:
                    bulk.insert( doc )
                except Exception as insert_exp:
                    self.logger.error("Error attempting to insert document from %s.%s document=%s" % (db,collection,doc))
                    self.logger.error("%s" % insert_exp)
                    continue
                doc_count += 1
                if (doc_count == batch_size ):
                    try:
                        r = bulk.execute({ 'w' : 'majority' })
                        self.logger.debug("initial sync on %s.%s bulk.execute result=%s" % (db,collection,r))
                        if (self.initial_sync_initial_tombstone_not_written):
                            self.logger.info("initial_sync updating tombstone since we've written data")
                            op = { "__mongo-migrate__" : "INITIAL_SYNC" }
                            tombstone = get_tombstone(op,self.args)
                            update_tombstone(tombstone,self.logger)
                            self.initial_sync_initial_tombstone_not_written = False
                    except BulkWriteError as bwe:
                        self.logger.error("bulk write error on %s.%s" % (db,collection))
                        self.logger.error(str(bwe))
                        self.logger.error("BulkWriteError.details=%s" % bwe.details)
                    doc_count=0
                    bulk = self.dest_mongo[db][collection].initialize_unordered_bulk_op()
            except StopIteration:
                self.logger.info('%s.%s source intial sync cursor complete' % (db,collection))
                if doc_count>0:
                    try:
                        r = bulk.execute({ 'w' : 'majority' })
                        self.logger.debug("initial sync on %s.%s result=%s" % (db,collection,r))
                    except BulkWriteError as bwe:
                        self.logger.error("bulk write error on %s.%x" % (db,collection))
                        self.logger.error(str(bwe))
                        self.logger.error("BulkWriteError.details=%s" % bwe.details)
                    self.logger.debug("initial sync on %s.%s result=%s" % (db,collection,r))

        # check if any more docs to send
        self.logger.info('sending last batch of doc_count=%i' % doc_count)
        if doc_count>0:
            try:
                r = bulk.execute()
                self.logger.debug("initial sync on %s.%s result=%s" % (db,collection,r))
            except BulkWriteError as bwe:
                self.logger.error("bulk write error on %s.%x" % (db,collection))
                self.logger.error(str(bwe))

        dest_count = self.dest_mongo[db][collection].count()
        self.logger.info('%s.%s dest_count=%s' % (db,collection,str(dest_count)))
        # create indexes
        self.logger.info('starting index migration for %s.%s' % (db,collection))
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
            c = self.dest_mongo[db][collection]
            options = dict(index)
            options.pop('name',None)
            options.pop('v',None)
            options.pop('ns',None)
            options.pop('safe',None)
            if not 'background' in options:
                options['background']=True
            try:
                r = c.create_index( pyindex,
                                name=index['name'],
                                **options)
                self.logger.info('create_index on %s.%s result:%s' % (db,collection,str(r)))
            except pymongo.errors.OperationFailure as op_fail:
                self.logger.error('create_index on %s.%s ERROR: %s' % (db,collection,str(op_fail)))
        try:
            self.logger.info("initial sync %s.%s: attempt to free resources"
                             % (db,collection))
            source_cursor.close()
            self.logger.info("initial sync %s.%s: resources free" % (db,collection))
        except Exception as exp:
            self.logger.error("Exception attempting to free resources")
            self.logger.error("%s" % exp)


    def get_last_source_oplog_entry(self):
        self.logger.debug('starting to fetch last oplog entry on source')
        self.source_mongo = get_mongo_connection(self.source)
        last_oplog_entry_cursor = self.source_mongo['local']['oplog.rs'].find({}).sort("ts",-1).limit(1)
        last_oplog_entry = last_oplog_entry_cursor.next()
        self.logger.debug('last oplog entry %s' % last_oplog_entry)
        last_oplog_entry_cursor.close()
        return last_oplog_entry


    def tail_oplog(self,start_ts=0):
        self.logger.info('tail_oplog')

        # start oplog_tasks_results monitor
        self.oplog_tasks_results_monitor =Thread(target=self.monitor_oplog_tasks_results,
                                                 args=(self.oplog_tasks_results,self.logger))
        self.oplog_tasks_results_monitor.setDaemon(True)
        self.oplog_tasks_results_monitor.start()
        self.oplog_consumer = OplogConsumer(self.args,self.logger,self.oplog_tasks,self.oplog_tasks_results,self.dest_mongo)
        try:
            self.oplog_consumer.start()
        except Exception as e:
            self.logger.error(str(e))
            traceback.print_exc()


        query = {}
        if (start_ts==0):
            try:
                last_oplog_entry = self.get_last_source_oplog_entry()
                query["ts"]={ "$gt" : last_oplog_entry['ts'] }
            except StopIteration:   #thrown when out of data so wait a little
                self.logger.info("local.oplog.rs"+' exists, but no entries found')
        else:
            query['ts']={ "$gt" : start_ts }
        if not len(self.nsToMigrate)==0:
            query['ns'] = { '$in' : self.nsToMigrate }
        self.logger.info('tail_oplog starting query=%s' % str(query))
        #start tailable cursor
        oplog = self.source_mongo['local']['oplog.rs'].find(query,cursor_type=CursorType.TAILABLE).batch_size(self.args.oplogBatchSize)
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
            except StopIteration:   #thrown when out of data so wait a little
                self.logger.debug("sleep %i" % self.args.tailSleepTimeSeconds)
                self.oplog_tasks.put( { "___.HeartbeatOp.___" : (datetime.datetime.now()) })
                time.sleep(self.args.tailSleepTimeSeconds)
                sleep_cycles += 1
                if (sleep_cycles % self.args.collectionCheckSleepCycles)==0:
                    self.logger.info("sleeping...")
                    self.run_collection_checks()

    def run_collection_checks(self):
        self.logger.info('Running collection checks')
        source_dbs = self.source_mongo['admin'].command('listDatabases')
        dest_dbs = self.dest_mongo['admin'].command('listDatabases')
        dbs_to_skip = [ 'admin', 'local']
        colls_to_skip = [ 'system.indexes' ]
        for database in source_dbs['databases']:
            db = database['name']
            if db in dbs_to_skip:
                continue
            source_colls = self.source_mongo[db].collection_names()
            for coll in source_colls:
                if coll in colls_to_skip:
                    continue
                if not self.should_migrate_ns(db,coll):
                    continue
                source_count = self.source_mongo[db][coll].count()
                dest_count = self.dest_mongo[db][coll].count()
                ok = ''
                if (source_count==dest_count):
                    ok = 'Looks good!'
                else:
                    ok = "ERROR!"
                    self.logger.error('%s.%s counts did not match!' % (db,coll))
                self.logger.info('%s.%s.count() source=%i, destination=%i %s'
                                 % (db,coll,source_count,dest_count,ok))


def get_tombstone(op,args,ts=datetime.datetime.now()):
    tombstone = { "last_op" : op,
                      "when" : ts,
                      "nsToMigrate" : args.nsToMigrate,
                      "v" : tool_version(),
                      "source" : args.source }
    return tombstone

def update_tombstone(tombstone,logger):
    temp_file = OPLOG_TOMBSTONE_FILE + ".temp"
    logger.debug("update_tombstone: updating file=%s with %s" % (temp_file,str(tombstone)))
    with open( temp_file, "w+", buffering=1) as f:
        try:
            f.write( dumps( tombstone ) )
        except Exception as exp:
            logger.error("update_tombstone: %s" % str(exp))
            raise exp
        finally:
            logger.debug("update_tombstone: attempting to rename '%s' to '%s')" % (temp_file,OPLOG_TOMBSTONE_FILE))
            os.rename(temp_file,OPLOG_TOMBSTONE_FILE)
            logger.debug("update_tombstone: complete")

def get_mongo_connection(uri):
    return pymongo.MongoClient(uri,connect=False,w="majority")

# deal with special characters in password
def deal_with_mongo_connection_string(uri):
    print uri

    parsed_cs = pymongo.uri_parser.parse_uri(uri)
    print str(parsed_cs)
    parsed_cs['password'] = urllib.quote_plus( parsed_cs['password'] )
    return parsed_cs


filehash = hashlib.md5()
filehash.update(open(__file__).read())
__tool_version__ = filehash.hexdigest()
def tool_version():
    return __tool_version__


def main():

    # parse arguments
    description = u'mongo-migrate - replica set synchonization tool'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--version",action='store_true',default=False,help='print version and exit')
    parser.add_argument("--source",help='mongodb uri for source replica set')
    parser.add_argument("--destination",help='mongodb uri for destination replica set')
    parser.add_argument("--oplogOnly",action='store_true',default=False,help='only stream oplog from source to destination')
    parser.add_argument("--skipTombstoneVersionCheck",action='store_true',default=False,help='allow tombstone file from another version of mongo-migrate.py, may fail if tombstone format changes')
    parser.add_argument("--initialSyncOnly",action='store_true',default=False,help='only all data from source to destination, do not append oplog entries')
    parser.add_argument("--dropOnDestination",action='store_true',default=False,help='drop collections on destination, default False')
    parser.add_argument("--migrateEmptyCollections",action='store_true',default=False,help='set to migrate empty collections, default False')
    parser.add_argument("--nsToMigrate",default='',help='comma delimited list of namespaces to sync, other namespaces are ignored')
    parser.add_argument("--loglevel",default='info',help='loglevel debug,info default=info')
    parser.add_argument("--logfile",default='--',help='logfile full path or -- for stdout')
    parser.add_argument("--numInsertionWorkers",type=int,default=5,help='number of threads run parallel initial sync for collections, default 5')
    parser.add_argument("--oplogBatchSize",type=int,default=1000,help='number of docs to pull from oplog at onetime')
    parser.add_argument("--tailSleepTimeSeconds",default=5,type=int,help='seconds to sleep when source oplog does not have new data, default 5')
    parser.add_argument("--collectionCheckSleepCycles",default=12,type=int,help='run collection checks after this many oplog tailing sleep cycles, default 12')
    parser.add_argument("--initialSyncBatchSize",default=1000,type=int,help='Batch size for initial sync, default 1000')
    args = parser.parse_args()
    logger = logging.getLogger("mongo-migrate")
    logger.setLevel(getattr(logging,args.loglevel.upper()))
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s")
    if args.logfile == '--':
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(os.path.abspath(args.logfile))
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(description)
    logger.info('mongrate startup')
    logger.info('version: %s' % tool_version())
    logger.debug("args: " + str(args))
    logger.info("log level set to " + logging.getLevelName(logger.getEffectiveLevel()))
    if (args.version):
        os._exit(0)
    if not args.source:
        logger.error("mongo-migrate.py: error: argument --source is required")
        os._exit(1)
    if not args.destination:
        logger.error("mongo-migrate.py: error: argument --destination is required")
        os._exit(1)

    app = App(args, logger)
    logger.info('mongo-migrate initialized')
    try:
        logger.info('running...')
        if app.args.oplogOnly:
            ts = app.check_oplog_tombstone()
            logger.debug('check_oplog_tombstone returned ts=%s' % str(ts))
            app.tail_oplog(ts)
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

