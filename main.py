#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
"""
    # 21/03 03:00

    * I'm concerned that adding changes to an `OmniSharer` isn't nice
        enough, and maybe be liable to generating deltas without the
        appropriate "update" operation
    * Take another look at the `OmniDB.append` code, should there be
        a friendlier API for adding changes to a database? Via `OmniSharer`?
        - update_node
        - update_task
        - add_task
    * A lot of the XML-traversal code is currently pseudo and probably
        won't work
    * It may be worth adding an `OmniDBNode` object to handle traversal
        in a nicer manner (means hacking about with __getattr__ though :/)
    * The GTDDB class needs improving somewhat to handle assigned and
        delegated tasks
"""
from copy import copy
from datetime import datetime
from glob import glob, iglob
import plistlib
import string
import sqlite3
import random
from zipfile import ZipFile

from lxml import etree


class GTDDB(object):
    """ Basic Interface to the GTDTogether Database. """
    tables = ('config', 'delegate_contexts')

    def __init__(self, username):
        self.username = None
        self.conn = sqlite3.connect('db.sqlite')
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        for table in GTDDB.tables:
            setattr(self, table, object())
            cursor.execute('SELECT * FROM %s WHERE username=?' % table, (self.username,))
            row = cursor.fetchone()
            for col in row.keys():
                setattr(getattr(self, table), col, row[col])


class OmniDate(datetime):
    """ Simple wrapper around `datetime` that
        prints in an OmniFriendly format.
    """
    @property
    def filename(self):
        return self.strftime('%Y%m%d%H%M%S')

    @property
    def xml(self):
        return '%sZ' % self.isoformat()[:-3]


class OmniDB(object):
    def __init__(self, username):
        self.path = 'dbs/%s/OmniFocus.ofocus' % username
        self.username = username
        self.main = None
        self.delta = None

    @property
    def root(self):
        try:
            return self.main.getroot()
        except AttributeError:
            raise OmniDB.NotReady

    def _load(self):
        self.last_id = glob('%s/*=GTDTogether.client' % self.path)[-1].split('/')[-1].split('=')[0]
        main, deltas = [], []
        stack = main
        for zfile in iglob('%s/*.zip' % self.path):
            stack.append(etree.parse(ZipFile(zfile).open('contents.xml'), etree.XMLParser(remove_blank_text=True)))
            if zfile.split('/')[-1].split('+')[1].split('.')[0] == self.last_id:
                stack = deltas
        self.main = main.pop(0)
        for tree in main:
            self._merge_delta(tree)
        self.delta = self.create_root()
        for tree in deltas:
            self._merge_delta(tree, self.delta)

    def reload(self):
        """ Undo all merged yet uncommitted changes from `main`
            and reload the database upto the last known point.
        """
        self._load()
        return self

    def commit(self):
        """ Commit all changes to the OmniFocus database. """
        self._generate_delta()   # generate deltas for `self.changes`
        self.reload()            # reload `self.main` and `self.delta` (incorporating new changes)
        self.merge()             # merge `self.delta` into `self.main` and write .client file
        return self

    def _generate_delta(self):
        """ Generate a delta file for each change
            and then a client file.
        """
        id = self._generate_id()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = '%s/%s=%s+%s.zip' % (self.dbpath, timestamp, self._last_id, id)
        root = self.create_root()
        for node in self.changes:
            root.append(node)
        zf = ZipFile(filename, 'w')
        zf.writestr('contents.xml', etree.tostring(root, encoding='utf-8', standalone=False))
        zf.close()
        self.client.generate(timestamp, id)

    def merge(self):
        """ Merge the unknown deltas into `main` and generate a .client file """
        self._merge_delta(self.delta)
        self.delta = self.create_root()
        self.client.create_file(datetime.now(), self.last_id)
        self.sql.config.last_id = 'TODO'  ## TODO!
        return self

    def _merge_delta(self, delta, base=None):
        """ Merge `delta` into `base`. """
        if base is None:
            base = self.root
        for node_type in ('context', 'task'):
            for el in self.xpath('/of:omnifocus/of:%s' % node_type, delta):
                op = el.attrib.get('op', None)
                if op == 'update':
                    # if the task has op=update then replace
                    # the task element in the db with the new element
                    try:
                        orig = self.xpath("/of:omnifocus/of:%s[@id='%s']" % (node_type, el.attrib['id']), base)[0]
                        base.remove(orig)
                    except IndexError:
                        pass
                    del(el.attrib['op'])
                    base.append(el)
                elif op is None:
                    # no operation implies a new element
                    base.append(el)
                elif op == 'delete':
                    ## FIXME I suspect this is wrong, and the node is simply marked as deleted
                    base.remove(el)

    def insert(self, el):
        """ This should only be used when you know you are
            inserting a new element into the database.
        """
        self.root.append(el)
        self.changes.append(el)
        return self

    def remove(self, el):
        """ Remove an element from `self.main` and
            add an op=delete change.
        """
        self.root.remove(el)
        delta = copy(el)
        delta.attrib['op'] = 'delete'
        self.changes.append(el)
        return self

    def _generate_id(self):
        """ Generate a unique OmniFocus ID. """
        id = ''.join(random.choice(string.ascii_letters) for i in xrange(11))
        if self.xpath("//[@id='%s']" % id):
            return self._generate_id()
        return id

    def create_context(self, name, idref=None):
        """ Create a context node """
        ctx = etree.Element('context')
        ctx.attrib['id'] = self._generate_id()
        added_el = etree.SubElement(ctx, 'added')
        added_el.text = '%s' % OmniDate(datetime.now())
        ctx.append(added_el)
        name_el = etree.SubElement(ctx, 'name')
        name_el.text = name
        ctx.append(name_el)
        rank_el = etree.SubElement(ctx, 'rank')
        rank_el.text = '0'
        ctx.append(rank_el)
        return ctx

    def create_root(self):
        """ Create a root <omnifocus /> node with all
            the required attributes.
        """
        root = etree.Element('omnifocus')
        root.attrib['xmlns'] = 'http://www.omnigroup.com/namespace/OmniFocus/v1'
        root.attrib['app-id'] = 'com.omnigroup.OmniFocus'
        root.attrib['app-version'] = '77.41.6.0.121031'
        root.attrib['os-name'] = 'NSMACHOperatingSystem'
        root.attrib['os-version'] = '10.6.2'
        root.attrib['machine-model'] = 'Xserve3,1'
        return root

    def xpath(self, query, base=None):
        if base is None:
            base = self.root
        return etree.xpath(base, query, namespaces={})


class OmniClient(object):
    client_id = 'GTDTogether'
    mac_addr = 'de:ad:be:ef:ca:fe'

    def generate_file(self, timestamp, id):
        """ Generate a .client file. """
        values = {
            'HardwareCPUCount': 2,
            'HardwareCPUType': '7,4',
            'HardwareCPUTypeDescription': 'Intel 80486',
            'HardwareCPUTypeName': 'i486',
            'HardwareModel': 'Xserve3,1',
            'OSVersion': '10C540',
            'OSVersionNumber': '10.6.2',
            'bundleIdentifier': 'com.omnigroup.OmniFocus',
            'bundleVersion': '77.41.6.0.121031',
            'clientIdentifier': OmniClient.client_id,
            'hostID': OmniClient.mac_addr,
            'lastSyncDate': '%sZ' % OmniDate(self.config.regdate).xml,  ## TODO this is wrong; parse in __init__
            'name': 'GTDTogether',
            'registrationDate': '%sZ' % OmniDate(self.config.regdate).xml,
            'tailIdentifiers': [id],
        }
        plistlib.writePlist(values, '%s/%s=%s.client' % (self.sharer.db.path, int(timestamp.filename) + 1, OmniClient.client_id))

    def parse_file(self, filename):
        """ Parse the plist body of a .client file. """
        pass


class OmniDelegateManager(object):
    _contexts = {
        'incoming': 'Incoming',
        'pending': 'Delegate',
        'accepted': 'Accepted',
        'declined': 'Declined',
        'completed': 'Complete',
    }
    contexts = {}

    def __init__(self, sharer):
        self.sharer = sharer
        self._load()

    def _load(self):
        """ Load the required delegation contexts, creating them
            if they do not exist.
        """
        def get_or_create(id, name, idref=None):
            try:
                if id is None:
                    raise OmniDB.ElementNotFound()
                el = self.sharer.db.get('context', id)
            except OmniDB.ElementNotFound:
                el = self.sharer.db.create_context(name, id, idref)
            return el
        ids = {}
        # Load any knows IDs from the database
        for key in OmniDelegateManager._contexts.iterkeys():
            ids[key] = getattr(self.sql.delegate_contexts, key)
        # Populate `self.contexts` with OmniDelegateContext instances, creating contexts if necessary
        self.contexts['root'] = OmniDelegateContext(get_or_create(ids['root'], 'GTD Together™'))
        ids['root'] = self.sql.delegate.contexts.root = self.contexts['root'].el.attrib['id']
        for key, id in ids.iteritems():
            self.contexts[key] = OmniDelegateContext(get_or_create(id, OmniDelegateManager._contexts[key], ids['root']))
        # Update the database with any amended IDs
        for key in OmniDelegateManager._contexts.iterkeys():
            setattr(self.sql.delegate_contexts, key, ids[key])


class OmniDelegateContext(object):
    """ Convenience Class to automate the creation of required delegate contexts. """
    def __init__(self, el):
        self.el = el

    @property
    def children(self):
        """ Query the DB for all direct descendents of this context. """
        return self.sharer.db.xpath("//of:task/of:task[@idref='%s']/.." % self.el.attrib['id'])

    def __getitem__(self, username):
        """ Allow use of context[username] to get/create a delegate context. """
        for el in self.children:
            if el.name.text == ('@%s' % username):
                return el
        el = self.sharer.db.create_context('@%s' % username, idref=self.el.attrib['id'])
        self.sharer.db.generate_delta([el])
        return OmniDelegateContext(el)


class OmniSharer(object):
    """ High level class for interacting with OmniFocus databases
        and handling delegated tasks.
    """
    db = None
    delta = None
    delegate = None

    def __init__(self, username):
        self.sql = GTDDB(username)
        self.client = OmniClient(self)
        self.db = OmniDB(username, self.client)
        self.delegate = OmniDelegateManager(self)

    def parse(self):
        """ Parse new changes to the database and take
            appropriate action for any delegated changes.
        """
        contexts = dict((el.attrib['id'], el) for rdc in self.delegate.itervalues() for el in rdc.itervalues())
        for task in self.db.tasks:
            if task.context.attrib['idref'] in contexts.values():
                type = contexts[task.context.attrib['idref']].parent.type
                target = OmniSharer(contexts[task.context.attrib['idref']].name[1:])
                if type == 'delegated':
                    # this should be in `target.incoming_task`
                    el = copy(task)
                    el.context.attrib['idref'] = target.delegate[type][target.username]
                    target.db.append(el)
                    target.db.commit()
        self._track_tasks()

    def _track_tasks(self):
        """ Track changes to known delegated tasks. """
        for delegator in self.sql.assigned_tasks.iterkeys():
            target = OmniSharer(delegator)
            for task_id in self.sql.assigned_tasks[delegator]:
                try:
                    task = self.db.get("//task[@id='%s']" % id, self.delta)
                except OmniDB.ElementNotFound:
                    continue
                # possible actions here? Accepted, Declined, Completed, (Updated)
                ## TODO updates to task, namely the notes field, need to be replicated
                el = None
                if task.attrib['op'] == 'delete':
                    el = target.db.get("//of:task[@id='%s']" % task.attrib['id'])
                    el.context.attrib['idref'] = target.delegate['declined'][self.username]
                if task.attrib['op'] == 'update':
                    if task.completed:
                        el = copy(task)
                        el.context.attrib['idref'] = target.delegate['completed'][self.username]
                    elif task.context.attrib['idref'] != self.delegate['incoming'][target.username]:
                        el = copy(task)
                        el.context.attrib['idref'] = target.delegate['accepted'][self.username]
                if el is not None:
                    target.db.append(el)
                    target.db.commit()
