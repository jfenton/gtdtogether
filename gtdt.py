import sqlite3
import unittest


class GTDTDbRow(object):
    controller = None
    table = None

    def __init__(self, controller, table):
        self.__dict__['controller'] = controller
        self.__dict__['table'] = table

    def __getattr__(self, col):
        return self.controller.fetch(self.table, col)

    def __setattr__(self, col, value):
        self.controller.update(self.table, col, value)


class GTDTDbRowSet(GTDTDbRow):
    """ Limited class that handles many rows. """
    def __getattr__(self, col):
        """ Returns an iterator for all values of 'col'. """
        return self.controller.fetchall(self.table, col)

    def __setattr__(self, col, values):
        self.controller.delete(self.table)
        for value in values:
            self.controller.insert(self.table, col, value)

    def append(self, **kwargs):
        """ Add a row to the set, currently this only supports
            one row per column, but that's all we need.
        """
        for col, value in kwargs.iteritems():
            self.controller.insert(self.table, col, value)


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
            setattr(self, table, cls(self, table))

    def update(self, table, col, value):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE %s SET %s=? WHERE username=?' % (table, col), (value, self.username))
        cursor.close()
        self.conn.commit()

    def insert(self, table, col, value):
        cursor = self.conn.cursor()
        cursor.execute('INSERT INTO %s (%s, username) VALUES (?, ?)' % (table, col), (value, self.username))
        cursor.close()
        self.conn.commit()

    def delete(self, table):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM %s WHERE USERNAME=?' % table, (self.username,))
        self.conn.commit()

    def fetch(self, table, col):
        cursor = self.conn.cursor()
        cursor.execute('SELECT %s FROM %s WHERE username=? LIMIT 1' % (col, table), (self.username,))
        return cursor.fetchone()[col]

    def fetchall(self, table, col):
        cursor = self.conn.cursor()
        cursor.execute('SELECT %s FROM %s WHERE USERNAME=?' % (col, table), (self.username,))
        return (row[col] for row in cursor.fetchall())


class GTDTDbTest(unittest.TestCase):
    def setUp(self):
        self.sql = GTDTDb('_test')

    def test_row_insert(self):
        self.sql.delegate_contexts.root = 'root_id'
        self.assertEqual(self.sql.delegate_contexts.root, 'root_id')

    def test_set_insert(self):
        self.sql.tracked_tasks.task_id = range(25)
        self.assertEqual([int(id) for id in self.sql.tracked_tasks.task_id], range(25))

    def test_set_append(self):
        self.sql.tracked_tasks.task_id = range(25)
        self.sql.tracked_tasks.append(task_id=100)
        self.assertEqual([int(id) for id in self.sql.tracked_tasks.task_id], range(25)+[100])


if __name__ == '__main__':
    unittest.main()
