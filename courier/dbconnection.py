import psycopg2
import yaml


class DBConnection:
    def __init__(self):
        with open('db_config.yaml ') as read_config:
            config = yaml.load(read_config, Loader=yaml.FullLoader)
        self.host = config["HOST"]
        self.port = config["PORT"]
        self.db = config["DB_NAME"]
        self.user = config["DB_USER"]
        password = open("db_pass.txt", "r")
        self.password = password.readline()
        self.connect = None
        self.connection()

    def connection(self):
        self.connect = psycopg2.connect(host=self.host, port=self.port, database=self.db, user=self.user,
                                        password=self.password)

    def execute_update(self, query, out=False):
        cur = self.connect.cursor()
        cur.execute(query)
        self.connect.commit()
        cur.close()
        return

    def execute_select(self, query):
        try:
            cursor = self.connect.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            while row:
                yield row
                row = cursor.fetchone()
            # return row
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.connect:
                cursor.close()
                self.connect.close()
                print("Соединение с PostgreSQL закрыто")

    def close_db(self) -> None:
        try:
            self.connect.close()
            print("Соединение с PostgreSQL закрыто")

        except (Exception, psycopg2.Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
