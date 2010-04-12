import sqlite3
import unittest


class GTDTDbRow(object):
    controller = None
    table = None
    id = None

    def __init__(self, controller, table, resultset):
        self.__dict__['controller'] = controller
        self.__dict__['table'] = table
        try:
            self.__dict__['rowid'] = resultset.next()
        except StopIteration:
            self.__dict__['rowid'] = self.controller.insert(self.table)

    def __getattr__(self, col):
        return self.controller.fetch(self.table, self.rowid, col)

    def __setattr__(self, col, value):
        self.controller.update(self.table, self.rowid, col, value)


class GTDTDbRowSet(GTDTDbRow):
    def __init__(self, controller, table, resultset):
        self.__dict__['controller'] = controller
        self.__dict__['table'] = table

    """ Limited class that handles many rows. """
    def __getattr__(self, col):
        """ Returns an iterator for all values of 'col'. """
        return self.controller.fetchall(self.table, col)

    def __setattr__(self, col, value):
        """ Setting attributes on sets of rows probably isn't desirable. """
        raise Exception

    def __len__(self):
        return self.controller.rowcount(self.table)

    def __iter__(self):
        """ An iterator over the rowset, returning GTDTDbRow objects. """
        for rowid in self.controller.fetchall(self.table, 'rowid'):
            yield GTDTDbRow(self.controller, self.table, (id for id in [rowid]))

    def all(self):
        """ Fetch all rows. """
        return [row for row in self]

    def append(self, **kwargs):
        """ Add a row to the set.
                `gtdtdbrowset.append(col1='val1', col2='val2')`
        """
        self.controller.insert(self.table, **kwargs)

    def delete(self, **kwargs):
        self.controller.delete(self.table, **kwargs)


class GTDTDb(object):
    """ Basic ORM for the GTDTogether Database. """
    tables = {
        'delegate_contexts': GTDTDbRow,
        'tracked_tasks': GTDTDbRowSet,
    }

    def __init__(self, username):
        self.username = username
        self.conn = sqlite3.connect('db.sqlite')
        self.conn.row_factory = sqlite3.Row
        for table, cls in GTDTDb.tables.iteritems():
            setattr(self, table, cls(self, table, self.fetchall(table, 'rowid')))

    def insert(self, table, **kwargs):
        if kwargs.has_key('rowid'):
            del(kwargs['rowid'])
        kwargs['username'] = self.username
        cursor = self.conn.cursor()
        cols = ', '.join(kwargs.iterkeys())
        values = ', '.join('?' for value in range(len(kwargs)))
        cursor.execute('INSERT INTO %s (%s) VALUES (%s)' % (table, cols, values), kwargs.values())
        cursor.close()
        self.conn.commit()
        return cursor.lastrowid

    def update(self, table, rowid, col, value):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE %s SET %s=? WHERE rowid=? AND username=?' % (table, col), (value, rowid, self.username))
        cursor.close()
        self.conn.commit()

    def delete(self, table, **kwargs):
        kwargs['username'] = self.username
        cursor = self.conn.cursor()
        query = 'DELETE FROM %s WHERE %s' % (table, ' AND '.join('%s=?' % col for col in kwargs.iterkeys()))
        cursor.execute(query, kwargs.values())
        self.conn.commit()

    def rowcount(self, table, rowid=None):
        cursor = self.conn.cursor()
        query = 'SELECT COUNT(username) AS rowcount FROM %s WHERE username=?' % table
        params = (self.username,)
        if rowid:
            query = '%s AND rowid=?'
            params = params + (rowid,)
        cursor.execute(query, params)
        return int(cursor.fetchone()['rowcount'])

    def fetch(self, table, rowid, col):
        cursor = self.conn.cursor()
        cursor.execute('SELECT %s FROM %s WHERE rowid=? AND username=? LIMIT 1' % (col, table), (rowid, self.username,))
        return cursor.fetchone()[col]

    def fetchall(self, table, col):
        cursor = self.conn.cursor()
        cursor.execute('SELECT %s FROM %s WHERE USERNAME=?' % (col, table), (self.username,))
        return (row[col] for row in cursor.fetchall())

    def purge(self):
        cursor = self.conn.cursor()
        for table in GTDTDb.tables.iterkeys():
            cursor.execute('DELETE FROM %s WHERE username=?' % table, (self.username,))
        cursor.close()
        self.conn.commit()


class GTDTDbTest(unittest.TestCase):
    def setUp(self):
        self.sql = GTDTDb('_test')

    def tearDown(self):
        self.sql.purge()

    def test_purge(self):
        self.sql.purge()
        for table in GTDTDb.tables.iterkeys():
            self.assertEqual(self.sql.rowcount(table), 0)
        # calling a second time tests loading an existing database
        self.sql = GTDTDb('_test')
        self.sql = GTDTDb('_test')

    def test_row_insert(self):
        self.sql.delegate_contexts.root = 'root_id'
        self.assertEqual(self.sql.delegate_contexts.root, 'root_id')

    def test_set_delete(self):
        self.sql.tracked_tasks.delete()
        self.assertEqual(len(self.sql.tracked_tasks), 0)

    def test_set(self):
        self.sql.tracked_tasks.delete()
        self.sql.tracked_tasks.append(delegator='_delegator', task_id='task_id')
        self.assertEqual(len(self.sql.tracked_tasks), 1)
        row = self.sql.tracked_tasks.all()[0]
        self.assertEqual(row.delegator, '_delegator')
        self.assertEqual(row.task_id, 'task_id')
        self.sql.tracked_tasks.append(delegator='_delegator2', task_id='task_id2')
        self.assertEqual(len(self.sql.tracked_tasks), 2)
        self.sql.tracked_tasks.append(delegator='_delegator3', task_id='task_id3')
        self.assertEqual(len(self.sql.tracked_tasks), 3)


if __name__ == '__main__':
    unittest.main()
