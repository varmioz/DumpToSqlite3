#! python3

import gzip, time, re, shutil, sqlite3
from os import remove
import xml.etree.ElementTree as ET

integer = re.compile(r'^[-+]?[0-9]+$')
real = re.compile(r'^[-+]?[0-9]+\.[0-9]*$')
sanitize = re.compile('[^0-9a-zA-Z_]+')

DumpName = 'RAN1_20180924.xml.gz'


def predict_type(value):
    if integer.match(value):
        return "INTEGER"
    if real.match(value):
        return "REAL"
    return "TEXT"


class Reader:
    def __init__(self, path, inplace=False):
        """
        :param path: path to file
        :param inplace: if true, the file will be decompressed in time, else file will be
                        fully decompressed and after that decompressed file will be read.
                        In the first situation, parsing will be slower than in second
        """
        self.ns = ''
        if path.split('.')[-1] == 'xml':
            file = path
        elif path.split('.')[-1] == 'gz':
            if inplace:
                file = gzip.GzipFile(path)
            else:
                new_path = '.'.join(path.split('.')[:-1])
                with open(new_path, 'wb') as f_in:
                    with gzip.open(path, 'rb') as f_out:
                        shutil.copyfileobj(f_out, f_in)
                file = new_path
        else:
            raise ValueError("Supports only xml and gz files")
        self.tree = ET.iterparse(file, events=('start-ns', 'start', 'end'))

        self.tables = dict()

        self.db = None
        self.cursor = None

        self.process_tree()

        self.db.commit()

    def process_tree(self):
        for event, element in self.tree:
            if event == 'start-ns':
                # getting namespace namespace
                self.ns = element[1]

            elif event == 'start':
                if element.tag == self.get_tag('header'):
                    name = DumpName[:DumpName.find('.')]+'.sqlite3'
                    self.db = sqlite3.connect(name)
                    self.cursor = self.db.cursor()
                    continue

                elif element.tag == self.get_tag('managedObject'):
                    self.process_managed_object(element)

                    element.clear()

    def process_managed_object(self, root):

        obj = {}
        table = root.attrib['class']
        primary_str = root.attrib.get('distName', "/")
        primary_elements = self.get_primary_keys(primary_str)

        obj.update(primary_elements)

        # Loop over elements of managedObject
        for event, element in self.tree:

            # exit condition
            if event == 'end' and element.tag == self.get_tag('managedObject'):

                # Skip situation when element doesn't have any elements
                if len(element) == 0:
                    return

                if table not in self.tables:
                    self.create_table(table, obj, primary_elements)

                self.insert_value(table, obj)
                # print(obj) # insert statement

                # Deleting all preprocessed tree elements
                # Needed for reducing memory usage
                root.clear()
                return

            elif event == 'start' and element.tag == self.get_tag('list'):
                value = self.process_list(element, table, primary_elements)

                # for lists without attribute name.
                # If value is None it's means that list is a different table
                if value is not None:
                    name = element.attrib['name']
                    obj[name] = value

            elif event == 'start' and element.tag == self.get_tag('p'):
                self.set_element(obj, element)

    def process_list(self, root, table, primary_elements):
        table = "{}_{}".format(table, root.attrib['name'])
        primary_elements = primary_elements.copy()

        for event, element in self.tree:
            if event == 'end' and element.tag == self.get_tag('list'):
                root.clear()
                return

            elif event == 'start' and element.tag == self.get_tag('item'):
                self.process_list_item(table, primary_elements)

            elif event == 'start' and element.tag == self.get_tag('p'):
                return '{} {}'.format(element.text if element.text else '', self.get_options()).strip()

    def process_list_item(self, table, primary_elements):
        primary_elements = primary_elements.copy()
        obj = {}
        obj.update(primary_elements)

        for event, element in self.tree:
            if event == 'end' and element.tag == self.get_tag('item'):
                if table not in self.tables:
                    self.create_table(table, obj, {})

                self.insert_value(table, obj)
                return

            elif event == 'start' and element.tag == self.get_tag('p'):
                self.set_element(obj, element)

    @staticmethod
    def sanitize(string):
        if string == "NULL":
            return "NULL"
        predicted_type = predict_type(string)
        if predicted_type == "TEXT":
            return "'{}'".format(string)  # sanitize_text.sub(' ', string))
        return string

    def create_table(self, table, obj, primary_elements):

        # creating list with all columns where primaries are the first columns
        primary_columns = list(primary_elements.keys())
        all_columns = primary_columns + list(obj.keys() - set(primary_columns))

        # Save those columns for inserting row elements in right order
        self.tables[table] = all_columns

        # Build SQL query for creating table
        columns_str = ", ".join("{} {}".format(key, predict_type(obj[key]))
                                for key in all_columns)

        if primary_elements:
            columns_str += ', PRIMARY KEY({})'.format(', '.join(primary_columns))
        sql_query = "CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        sql_query = sql_query.format(table_name=table, columns=columns_str)
        try:
            self.cursor.execute(sql_query)
        except Exception as e:
            print(e)
            print(sql_query)
            exit(1)

    def insert_value(self, table, obj):

        # Building query for inserting value
        columns = self.tables[table]
        values = [self.sanitize(obj.get(key, 'NULL')) for key in columns]

        columns = ', '.join(columns)

        sql_query = "INSERT OR REPLACE INTO {table} ({column_list}) VALUES({value_list});"
        sql_query = sql_query.format(table=table, column_list=columns, value_list=', '.join('?' * len(values)))
        try:
            self.cursor.execute(sql_query, values)
        except Exception as e:
            print(e)
            print(sql_query)
            exit(1)

    def set_element(self, obj, element):
        name = element.attrib['name']
        value = element.text

        # Dirty hack. Don't change this. It will save you a lot of time.
        if value is None:
            _, element = next(self.tree)
            value = element.text

        obj[name] = value

    def get_tag(self, tag):
        return "{{{ns}}}{tag}".format(ns=self.ns, tag=tag)

    def get_date(self):
        date = None
        for event, element in self.tree:

            if event == 'end' and element.tag == self.get_tag('header'):
                if date is None:
                    raise ValueError("Date not found in header!")
                return date

            elif event == 'start' and element.tag == self.get_tag('log'):
                date = element.attrib['dateTime']

    def get_options(self):
        tree = self.tree
        result = []
        for event, element in tree:
            if event == 'end' and element.tag == self.get_tag('list'):
                return ' '.join(result)

            elif event == 'start' and element.tag == self.get_tag('p'):
                if element.text is None:
                    try:
                        element = next(element)
                        result.append(element.text)
                    except (TypeError, StopIteration):
                        pass
                else:
                    result.append(element.text)

    @staticmethod
    def get_primary_keys(string):
        values = string.split('/')
        keys = {}
        for value in values[1:]:
            key = '_'.join(value.split('-')[:-1])
            key = sanitize.sub('_', key)
            value = value.split('-')[-1]
            keys[key] = value
        return keys


if __name__ == "__main__":

    start_time = time.time()
    print('Start processing '+ DumpName)
    Reader(path=DumpName, inplace=False)
    minutes, seconds = divmod(int(time.time() - start_time), 60)
    if '.gz' in DumpName:
        remove(DumpName[:DumpName.rfind('.')])
        print('Temporary xml file removed')
    print('%s parsed for %s min %s sec.' % (DumpName, str(minutes), str(seconds)))