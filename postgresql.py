from configparser import ConfigParser
import psycopg2


class postgresql(object):
    """postgresql db connection"""

    insert_result_query = """ INSERT INTO simulation (name, key, code, start_date, end_date, duration, input1, input2, input3, unmatch, success, fail) VALUES (%s,%s,%s,%s,%s,%s%s,%s,%s,%s,%s,%s)"""

    def __init__(self):
        self.database_config = self.config()
        self.connector = None

    def config(self, filename='db.ini', section='postgresql'):
        parser = ConfigParser()
        parser.read(filename)
        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        return db

    def connect(self):
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        return psycopg2.connect(**self.database_config)

    def insert_result(self, name, key, code, start_date, end_date, duration, input1, input2, input3, unmatch, success,
                      fail):
        try:
            print('insert to the PostgreSQL database...')
            cursor = self.connector.cursor()
            cursor.execute(self.insert_result_query, (
            name, key, code, start_date, end_date, duration, input1, input2, input3, unmatch, success, fail))
            cursor.commit()
        except (Exception, psycopg2.Error) as error:
            print(f'error - {error}')
        finally:
            cursor.close()

    def __enter__(self):
        self.connector = self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connector is not None:
            self.connector.close()
            print('Database connection closed.')