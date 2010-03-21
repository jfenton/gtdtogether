from glob import iglob
import re
from zipfile import ZipFile

from lxml import etree


# shortcut function because of the stupid xpath namespace stuff
xpath = lambda xml, xpath: xml.xpath(xpath, namespaces={'of':"http://www.omnigroup.com/namespace/OmniFocus/v1"})

def merge_db(full=True):
    trees = []
    # parse the main xml and all the deltas in the database
    for zfile in iglob('OmniFocus.ofocus/*.zip'):
        trees.append(etree.parse(ZipFile(zfile).open('contents.xml'), etree.XMLParser(remove_blank_text=True)))
    main = trees.pop(0)
    if not full:
        nodes = xpath(main, '/of:omnifocus/*')
        for node in nodes:
            main.getroot().remove(node)
        ## TODO
        # remove known deltas from stack here
        # this will require creating .client
        # files and looking for them here
    for tree in trees:
        # find all the contexts and tasks in the deltas
        for node_type in ('context', 'task'):
            for el in xpath(tree, '/of:omnifocus/of:%s' % node_type):
                op = el.attrib.get('op', None)
                if op == 'update':
                    # if the task has op=update then replace
                    # the task element in `main` with the new element
                    try:
                        orig = xpath(main, "/of:omnifocus/of:%s[@id='%s']" % (node_type, el.attrib['id']))[0]
                        main.getroot().remove(orig)
                    except IndexError:
                        pass
                    ## FIXME this smells like it'll go bad when full=False
                    # if we are preparing a delta for another's consumation
                    # we'll want to add the op=update dynamically
                    del(el.attrib['op'])
                    main.getroot().append(el)
                elif op == 'delete':
                    pass
                elif op is None:
                    # no operation implies a new element
                    main.getroot().append(el)
    return main

def find_shared_tasks(db):
    tasks = xpath(db, '/of:omnifocus/of:task')
    result = {}
    for task in tasks:
        try:
            note = xpath(task, './of:note//of:lit')[0].text
        except IndexError:
            continue
        rx = re.compile('\(((?:@|#)[^)]+)\)$')
        match = rx.search(note)
        if match:
            elements = [task]
            children = get_task_children(db, [task.attrib['id']])
            while children:
                child = children.pop(0)
                elements.append(child)
                children += get_task_children(db, child)
            for name in match.groups()[0].split(','):
                key = name.strip()
                result[key] = result.get(key, [])
                result[key] += elements
    return result

def get_task_children(db, ids):
    results = []
    for id in ids:
        for el in xpath(db, "//of:task[@idref='%s']" % id):
            results.append(el.getparent())
    return results

if __name__ == '__main__':
    db = merge_db(True)
    print etree.tostring(db, pretty_print=True)
    from pprint import pprint
    pprint(find_shared_tasks(db))
