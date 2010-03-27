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


    # 27/03 00:30
    A lot more stuff is working now, but I'm going to commit so I have
    a snapshot, as I think a lot of core stuff needs rejigging:

    * Loading of the XML database should properly follow the file ids
        and not just glob() over the .ofocus
        - hopefully doing this would result in some clarity over
            `OmniDB.last_id` and `OmniDB.last_delta_id`
    * XML Transactions are currently extremely buggy and pretty much
        don't work properly.
        - merge delta into main on load and generate a delta, but keep
            a record of the new changes to the db?
    * The OmniDelegateManager is currently inserting directly into it's
        parent OmniSharer's db.main; this doesn't feel very nice. Under a
        nicer transactional system, we could force a reload after we commit()
    * The DB structure and ORM need to be given some consideration in regards to
        tracked tasks.
            - username|task_id|delegatee?
    * `OmniClient` is still using placeholder values for a lot of stuff
"""
from copy import copy
from datetime import datetime
from glob import glob, iglob
import plistlib
import string
import random
from zipfile import ZipFile

from lxml import etree, objectify

from gtdt import GTDTDb


class OmniDate(datetime):
    """ Simple wrapper around `datetime` that
        prints in an OmniFriendly format.
    """
    @property
    def filename(self):
        """ Used in db filenames """
        return self.strftime('%Y%m%d%H%M%S')

    @property
    def xml(self):
        """ Used inside plists and the xml db """
        return '%sZ' % self.isoformat()[:-3]

class OmniNode(object):
    """ Generic class to provide an interface to the OmniFocus folder/task heirarchy. """
    def __init__(self, el):
        if el.get('idref'):
            try:
                el = self.xpath('/of:omnifocus/of:%s[@id="%s"]' % (el.tag, el.get('idref')))[0]
            except IndexError:
                return OmniDB.ElementNotFound
        self.el = el

    def xpath(self, query, base=None):
        # methinks using OmniDB instance methods like this is bad.
        return OmniNode(el for el in OmniDB.xpath(None, query, base))

    @property
    def id(self):
        return self.el.get('id')

    @property
    def name(self):
        return self.el.name.text

    @property
    def parent(self):
        """ Attempt to find a parent task, project, or folder. """
        if self.el.task:
            id = self.el.task.get('idref')
        else:
            try:
                id = self.project.folder.id
            except AttributeError:
                return None
        try:
            return self.xpath('/of:omnifocus/of:folder|of:task[@id="%s"]' % id)[0]
        except IndexError:
            pass
        return None

    @property
    def children(self):
        return self.xpath('//of:%s[@idref="%s"]/..' % (self.el.tag, self.el.get('id')))

    @property
    def path(self):
        """ Returns the "path" of a node

            "Company One : Project A : Task : Subtask" would return
            ['Company One', 'Project A', 'Task', 'Subtask']
        """
        ascendents = [self.name]
        parent = self.parent
        while parent:
            ascendents.append(parent.name)
            parent = self.parent
        return ascendents[::-1]

    @property
    def project(self):
        return OmniNode(self.el.project) or None

    @property
    def folder(self):
        return OmniNode(self.project.folder) or None

    def is_folder(self):
        return (self.el.tag == 'folder')

    def is_project(self):
        return (self.project is not None)


class OmniDB(object):
    def __init__(self, username, client):
        self.path = 'dbs/%s/OmniFocus.ofocus' % username
        self.username = username
        self.client = client
        self.main = None
        self.delta = None
        self.changes = []
        self._load()

    @property
    def root(self):
        try:
            return self.main.getroot()
        except AttributeError:
            raise OmniDB.NotReady

    def _load(self):
        try:
            self.last_id = self.client._get_last_sync()
        except OmniClient.NewDB:
            self.last_id = None
        main, deltas = [], []
        stack = main
        for zfile in iglob('%s/*.zip' % self.path):
            stack.append(objectify.parse(ZipFile(zfile).open('contents.xml'), etree.XMLParser(remove_blank_text=True)))
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
        self.reload()            # reload `self.main` and `self.delta` (discarding changes to self.main)
        self.merge()             # merge `self.delta` into `self.main` and write .client file
        self._generate_delta()   # generate deltas for `self.changes`
        self.reload()            # reload `self.main` and `self.delta` (incorporating new changes)
        return self

    def _generate_delta(self):
        """ Generate a delta file for each change
            and then a client file.
        """
        id = self._generate_id()
        timestamp = OmniDate.now()
        filename = '%s/%s=%s+%s.zip' % (self.path, timestamp.filename, self.last_id, id)
        root = self.create_root()
        while self.changes:
            root.append(self.changes.pop(0))
        zf = ZipFile(filename, 'w')
        zf.writestr('contents.xml', etree.tostring(root, encoding='utf-8', standalone=False))
        zf.close()
        self.client.generate_file(timestamp, id)
        self.last_id = id

    def merge(self):
        """ Merge the unknown deltas into `main` and generate a .client file """
        self._merge_delta(self.delta)
        self.delta = self.create_root()
        self.client.generate_file(OmniDate.now(), self.last_id or self.last_delta_id)
        self.last_id = self.client._get_last_sync()
        return self

    @property
    def last_delta_id(self):
        return glob('%s/*.zip' % self.path)[-1].split('/')[-1].split('+')[1].split('.')[0]

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
        """ Insert an element into `self.main`
            TODO this isn't update-safe; should it be?
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

    def xpath(self, query, base=None):
        if base is None:
            base = self.root
        return base.xpath(query, namespaces={'of': "http://www.omnigroup.com/namespace/OmniFocus/v1"})

    def get(self, node, id):
        try:
            return self.xpath("//of:%s[@id='%s']" % (node, id))[0]
        except IndexError:
            raise OmniDB.ElementNotFound

    def get_project(self, name, folder=''):
        if folder:
            folder = 'of:folder[@idref="%s"]/' % folder
        try:
            return self.xpath('//of:project/%s../task[@idref="%s"]/..' % (name, folder))[0]
        except IndexError:
            raise OmniDB.ElementNotFound

    def get_folder(self, name, parent=''):
        if parent:
            parent = 'of:folder[@idref="%s"]/' % parent
        try:
            return self.xpath('/of:omnifocus/of:folder/of:name[text()="%s"]..%s' % (name, parent))[0]
        except IndexError:
            raise OmniDB.ElementNotFound

    def _generate_id(self):
        """ Generate a unique OmniFocus ID. """
        id = ''.join(random.choice(string.ascii_letters) for i in xrange(11))
        if self.xpath("//*[@id='%s']" % id):
            return self._generate_id()
        return id

    def create_root(self):
        """ Create a root <omnifocus /> node with all
            the required attributes.
        """
        root = objectify.Element('omnifocus')
        root.set('xmlns', 'http://www.omnigroup.com/namespace/OmniFocus/v1')
        root.set('app-id', 'com.omnigroup.OmniFocus')
        root.set('app-version', '77.41.6.0.121031')
        root.set('os-name','NSMACHOperatingSystem')
        root.set('os-version','10.6.2')
        root.set('machine-model', 'Xserve3,1')
        return root

    def create_context(self, name, id=None, idref=None):
        """ Create a context node """
        ctx = objectify.Element('context')
        ctx.set('id', id or self._generate_id())
        if idref:
            ctx.context = None
            ctx.context.set('idref', idref)
        ctx.added = '%s' % OmniDate.now().xml
        ctx.name = name
        ctx.rank = 0
        return ctx

    class NotReady(Exception):
        pass

    class ElementNotFound(Exception):
        pass


class OmniClient(object):
    client_id = 'GTDTogether'
    mac_addr = 'de:ad:be:ef:ca:fe'

    def __init__(self, sharer):
        self.sharer = sharer

    def _get_last_sync(self):
        try:
            fn = glob('dbs/%s/OmniFocus.ofocus/*=GTDTogether.client' % self.sharer.username)[-1]
        except IndexError:
            raise OmniClient.NewDB
        pl = plistlib.readPlist(fn)
        try:
            return pl['tailIdentifiers'][0]
        except IndexError:
            raise Exception
        except KeyError:
            raise Exception
        raise Exception

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
            'lastSyncDate': '%sZ' % OmniDate.now().xml,  ## FIXME
            'name': 'GTDTogether',
            'registrationDate': '%sZ' % OmniDate.now().xml,  ## FIXME
            'tailIdentifiers': [id],
        }
        plistlib.writePlist(values, '%s/%s=%s.client' % (self.sharer.db.path, int(timestamp.filename) + 1, OmniClient.client_id))

    def parse_file(self, filename):
        """ Parse the plist body of a .client file. """
        pass

    class NewDB(Exception):
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
        self.db = OmniDB(sharer.username, sharer.client)
        self._load()

    def _load(self):
        """ Load the required delegation contexts, creating them
            if they do not exist.
        """
        if not self.sharer.sql.delegate_contexts.root:
            self._init()
        self.contexts['root'] = OmniDelegateContext(self.db.get('context', self.sharer.sql.delegate_contexts.root), self)
        for key in OmniDelegateManager._contexts.iterkeys():
            id = getattr(self.sharer.sql.delegate_contexts, key)
            if id:
                self.contexts[key] = OmniDelegateContext(self.db.get('context', id), self)
            else:
                self.contexts[key] = self._create_context(key, commit=False)
        self.db.commit()

    def _init(self):
        ctx = self.db.create_context(u'GTD Together™')
        self.sharer.sql.delegate_contexts.root = ctx.get('id')
        self.db.insert(ctx)
        self.db.commit()

    def _create_context(self, type, commit=True):
        """ Internal function used for creating root delegate contexts. """
        ctx = self.db.create_context(OmniDelegateManager._contexts[type], idref=self.contexts['root'].id)
        setattr(self.sharer.sql.delegate_contexts, type, ctx.get('id'))
        self.db.insert(ctx)
        self.sharer.db.root.append(ctx)
        if commit:
            self.db.commit()
        return OmniDelegateContext(ctx, self)


class OmniDelegateContext(object):
    """ Convenience Class to automate the creation of required delegate contexts. """
    # The context type (root, user) and delegation type (incoming, pending, accepted, declined, completed)
    _type = None
    # The direct parent context, if available
    _parent = None
    # The root delegate context
    _root = None
    # The context's path relative to it's root user node
    _path = None

    def __init__(self, el, manager):
        self.el = el
        self.manager = manager

    def __getitem__(self, key):
        """ Allow use of context[child] to seemlessly get/create a delegate context. """
        key = '@%s' % key if self.type[0] == 'root' else key
        for child in self.children:
            if child.el.name.text == key:
                return child
        return self.manaer._create_context(key, idref=self.el.attrib['id'])

    def new(self, el):
        """ Create a new OmniDelegateContext object with the same
            manager as this one.
        """
        return OmniDelegateContext(el, self.manager)

    @property
    def parent(self):
        """ Shortcut to parent context. """
        if self.type[0] == 'root':
            return None
        return self.new(self.sharer.db.xpath("//of:task[@id='%s']/.." % self.el.attrib['id'])[0])

    @property
    def root(self):
        """ Resolves the root context for this delegation type. """
        if self._root:
            return self._root
        parent = self.parent or self
        while parent.type != 'root':
            parent = parent.parent
        self._root = parent
        return self._root

    @property
    def type(self):
        """ A two-tuple containing the types of the Context.

            The first part indicates if the context is a root context
            or a user context:
                root context: "Delegate To"
                user context: "Delegate To : @user"
                user context: "Incoming : @user : Urgent"

            The second part indicates the type of delegation:
                incoming, pending, accepted, declined, completed
        """
        if self._type:
            return self._type
        for type, context in self.manager.contexts.iteritems():
            if self.root == context:
                self._type = (self == self.root and 'root' or 'user', type)
        return self._type

    @property
    def path(self):
        """ Returns the "path" of the Context

            "Incoming : @user : Tasks : Urgent" would return ['Tasks', 'Urgent']
        """
        if self._path:
            return self._path
        ascendents = [self.name]
        parent = self.parent
        while parent and not parent.isroot():
            ascendents.append(parent.name)
            parent = self.parent
        self._path = ascendents[1:][::-1]
        return self._path

    @property
    def id(self):
        return self.el.get('id')

    @property
    def name(self):
        """ Returns the context's name. """
        return self.el.name.text

    @property
    def username(self):
        """ Resolves the associated username if this is a
            user delegation context. Username is always None
            for a root delegation context.
        """
        if self.type[0] == 'root':
            return None
        el = self
        while not el.name.text.startswith('@'):
            el = el.parent
        return self.name.text[1:]

    @property
    def children(self):
        """ Query the DB for all direct descendents of this context. """
        for child in self.sharer.db.xpath("//of:task/of:task[@idref='%s']/.." % self.el.attrib['id']).iter():
            yield OmniDelegateContext.new(child)


class OmniSharer(object):
    """ High level class for interacting with OmniFocus databases
        and handling delegated tasks.
    """
    db = None
    delta = None
    delegate = None

    def __init__(self, username):
        self.username = username
        self.sql = GTDTDb(username)
        self.client = OmniClient(self)
        self.db = OmniDB(username, self.client)
        self.delegate = OmniDelegateManager(self)

    def parse(self):
        """ Parse new changes to the database and take
            appropriate action for any delegated changes.
        """
        print etree.tostring(self.db.delta, pretty_print=True)
        try:
            for task in self.db.delta.task:
                context = OmniDelegateContext(task.context, self.delegate)
                if not context.isroot() and context.root == self.delegate.pending:
                    target = OmniSharer(context.username)
                    el = copy(task)
                    el.context.set('idref', target.delegate.incoming[self.username].get('id'))
                    target.db.append(el).commit()
        except AttributeError:
            pass
        #self._track_tasks()

    def _track_tasks(self):
        """ Track changes to known delegated tasks. """
        # TODO FIXME
        # this doesn't work at all
        # I need to think about the db/orm here.
        for delegator in self.sql.tracked_tasks.iterkeys():
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

    def find_project(self, path):
        """ Find the best-match for an external project.
            `path` should be a list of project names ['Company', 'Website'].

            First a project of matching name is looked for, regardless of heirarchy.
            If one is found, that project is returned. If two or more are found, the
            one which matches `path` exactly will be is return, failing that `None`.

            TODO "Fuzzy" matching, if the paths share the same head/tail.
        """
        projects = self.xpath('/of:omnifocus/of:task/of:project[@id]/../of:name[text()="%s"]../of:project' % path[-1])
        if projects:
            return None
        elif len(projects) == 1:
            return projects[0]
        else:
            for project in projects:
                if OmniNode.path(project) == path:
                    return project
            return None


if __name__ == '__main__':
    OmniSharer('wrboyce').parse()
