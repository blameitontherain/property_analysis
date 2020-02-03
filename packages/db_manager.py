import pymysql.cursors, pprint

class DatabaseManager(object):
    def __init__(self, username, password, host, database):
        charset = "utf8mb4"
        self.pp = pprint.PrettyPrinter(indent=1)
        try:
            self.mysql_connection = pymysql.connect(host = host,
                                                    user = username,
                                                    password = password,
                                                    db = database,
                                                    charset = charset,
                                                    cursorclass = pymysql.cursors.DictCursor)
            self.cursor = self.mysql_connection.cursor()
        except Exception as e:
            print("## Attempt to connect to MySQL has produced the following error:", e, "##")


    def return_connection(self):
        return self.mysql_connection


    def list_databases(self):
        databases = []
        sql_statement = "SHOW DATABASES"
        self.execute_sql(sql_statement)
        [databases.append(db) for db in self.cursor]
        return self.pp.pprint(databases)


    def use_database(self, database):
        sql_statement = "USE %s" % database
        self.execute_sql(sql_statement)


    def show_tables(self, database):
        self.use_database(database)
        tables = []
        sql_statement = "SHOW tables"
        self.execute_sql(sql_statement)
        [tables.append(table) for table in self.cursor]
        return self.pp.pprint(tables)


    def describe_table(self, database, table):
        self.use_database(database)
        sql_statement = "DESC %s" % table
        self.execute_sql(sql_statement) 
        return self.pp.pprint(self.cursor.fetchall())


    def delete_table(self, database, table):
        self.use_database(database)
        sql_statement = "DROP TABLE %s" % table
        self.execute_sql(sql_statement)


    def create_database(self, db_name):
        sql_statement = "CREATE DATABASE %s" % db_name
        self.execute_sql(sql_statement)


    def execute_sql(self, sql_statement, data=None):
        return self.cursor.execute(sql_statement, data)


    def execute_fetchall(self, sql_statement, data=None):
        self.result = self.cursor.execute(sql_statement, data)
        self.fetch_result = self.cursor.fetchall()
        return self.fetch_result


    def execute_many(self, sql_statement, data=None):
        return self.cursor.executemany(sql_statement, data)


    def commit_changes(self):
        self.mysql_connection.commit()


    def close_connection(self):
        self.mysql_connection.close()

