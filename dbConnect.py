import pymysql.cursors

class Database:
    connectionHandler = []
    cursor = []
    def __init__(self, host, user, passwd, db, cursorclass = pymysql.cursors.DictCursor):
        """
        Creates an instance and connects to the specifeid database
        :param host: host location
        :param user: user
        :param passwd: pass
        :param db: database
        :param cursorclass: cursorclass: default pymysql.cursors.DictCursor
        """
        self.connectDataBase(host, user, passwd, db, cursorclass)

    def connectDataBase(self, host, user, passwd, db, cursorclass = pymysql.cursors.DictCursor):
        """
        Creates an instance and connects to the specifeid database
        :param host: host location
        :param user: user
        :param passwd: pass
        :param db: database
        :param cursorclass: cursorclass: default pymysql.cursors.DictCursor
        """
        try:
            self.connectionHandler = pymysql.connect \
                            (host=host,
                             user=user,
                             password=passwd,
                             db=db,
                             cursorclass=cursorclass)
            self.cursor =  self.connectionHandler.cursor()
        except BaseException:
            print("-could not connect to database")
        finally:
            print("-connected to database")

    def disconnectDataBase(self):
        """
        Disconect from Database
        :return: n/a
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        self.connectionHandler.close()
        self.connectionHandler = []



    def isConnected(self):
        """
        Check if the db connector is UP
        :return: true on success
        """
        return self.connectionHandler == []

    def createTable(self, name, primaryKey, *args):
        """
        Create a table: usage: primaryKey != None + fields : ex name varchar(30)
        :param name: table name
        :param primaryKey: primary key
        :param args: name + type - SQL specific
        :return: n/a
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        querry = "CREATE TABLE IF NOT EXISTS " + name + "("
        for arg in args:
            querry += arg + ", "
        if primaryKey is "None":
            query = querry[:-2] + ")"
        else:
            query = querry + "PRIMARY KEY (" + primaryKey + "));"
        print(query)
        self.cursor.execute(query)
        self.connectionHandler.commit()

    def deleteTable(self, name):
        """
        Delete table if exists
        :param name: table name
        :return:
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        query = "DROP TABLE " + name + ";"
        print (query)
        self.cursor.execute(query)
        self.connectionHandler.commit()


    def insert(self, table, col, val):
        """
        Insert a row with the following values defined in col and val in the same order
        :param table: talble name
        :param col: list of columns
        :param val: list of values
        :return:
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        if len(list(col)) != len(list(val)) and len(list(col)) < 1:
            return False
        query = "INSERT INTO " + table + " SET "
        res = zip(col, val)
        for k, v in res:
            query += k + "='" + v +"',"
        try:
            print (query);
            self.cursor.execute(query[:-1] + ";")
            self.connectionHandler.commit()
        except pymysql.err.IntegrityError:
            print("-duplicate entry")
        except pymysql.err.InternalError:
            print("-invalid values for entry")

    def delete(self, table, col, val):
        """
        Delete an entry from the table with the following conditions define in col, val
        :param table: table name
        :param col: list of columns
        :param val: list of values
        :return:
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        query = "DELETE FROM " + table + " WHERE "
        if not isinstance(col, (list, tuple)):
            query += col + "='" + val +"';"
        else:
            res = zip(col, val)
            for k, v in res:
                query += k + "='" + v + "' AND "
            query = query[:-4] + ";"
        try:
            print (query)
            self.cursor.execute(query)
            self.connectionHandler.commit()
        except pymysql.err.InternalError:
            print("-invalid values for entry")


    def displayEntries(self, table):
        """
        Show entries for the current table
        :param table: table name
        :return: n/a
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        query = "SELECT * FROM `record`"
        self.cursor.execute(query)
        print(self.cursor.fetchall())

    def displayEntriesCondition(self, table, col, val):
        """
        Show entries for the current table on condition specified in col + val
        :param table: table name
        :param col: list of columns
        :param val: list of values
        :return: n/a
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        query = "SELECT * from " + table + " WHERE " + col + "='" + val + "';"
        self.cursor.execute(query)
        self.connectionHandler.commit()
        print(self.cursor.fetchall())

    def displayEntriesSortedByField(self, table, col):
        """
        Display entries ordered by a 'col'
        :param table: table name
        :param col: column ordering
        :return: n/a
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        query = "SELECT * from " + table + " ORDER BY " + col + ";"
        self.cursor.execute(query)
        self.connectionHandler.commit()
        print(self.cursor.fetchall())

    def displayCustomQuerry(self, query):
        """
        Execute a valid SQL query
        :param query: VALID sql querry
        :return: n/a
        """
        if self.isConnected():
            print("-Not connected to db")
            return False
        try:
            self.cursor.execute(query)
            self.connectionHandler.commit()
            print(self.cursor.fetchall())
        except pymysql.err.Error:
            print("-invalid querry")

    def updateRowCondition(self, table, col, data, *args):
        """
        Change the *args for the entries that satisfy col + data
        :param table: table name
        :param col: single col name
        :param data: single data value
        :param args: list of arguments to be changed
        :return:
        """
        if len(args) < 1:
            print("No arguments provided")
            return False
        if self.isConnected():
            print("-Not connected to db")
            return False
        querry = "UPDATE " + table + " SET "
        for field in args:
            querry += field + ", "
        query = querry[:-2] + " WHERE " + col + " = " + data + ";"
        print(query)
        self.cursor.execute(query)
        self.connectionHandler.commit()


mydb = Database('localhost', 'root', 'a', 'db', pymysql.cursors.DictCursor)
mydb.insert('record', ('name', 'birthday', 'grade'), ('jomby', '1990-01-01', '4'))
mydb.insert('record', ('name', 'birthday', 'grade'), ('jony', '1990-01-01', '4'))
mydb.displayEntriesCondition('record', 'name', 'jonny')
mydb.displayEntries('record')
mydb.displayEntries('users')
mydb.displayEntriesSortedByField('record', 'grade')
mydb.displayCustomQuerry('select * from record;')
mydb.updateRowCondition('record', 'id', '5', "name='adi2'")
mydb.createTable("peNis", "aa", "aa varchar(20)", "bb int NOT NULL")
mydb.deleteTable("peNis")
mydb.delete('record', 'name', 'jony')
mydb.delete('record', ('name', 'birthday', 'grade'), ('jomby', '1990-01-01', '4')mydb.isConnected())
mydb.disconnectDataBase()
print(mydb.isConnected())