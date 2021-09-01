from config import Configuration
import pyodbc
import time


class SQLCaller(object):
    """Object for grouping all SQL calls used in pipeline"""

    def __init__(self):
        """Load in the sql uri from config object"""
        config = Configuration()
        self.sql_uri = config.settings['SQL_URI']

    def get_cursor(self) -> pyodbc.Cursor:
        """
        Generates a cursor object to interface with the SQL database specified in configuration settings.
        To deal with intermittent connection errors, the cursor connection will be attempted up to 50 times
        until a successful connection is created.
        :return: Cursor object for interfacing with database
        """
        cursor = None
        attempts = 0
        while cursor is None and attempts < 50:
            try:
                cursor = pyodbc.connect(self.sql_uri).cursor()
                return cursor
            except Exception as e:
                print(f'Cursor could not be generated: {e}')
                if attempts % 10 == 0:
                    time.sleep(15 * 60)
                attempts += 1
        assert type(cursor) == pyodbc.Cursor, "Unable to Establish Connection to SQL Database."

    def save_new_data(self, save_data: list, table: str):
        """
        Method to save new data collected by the pipeline.
        :param save_data: List of tuples corresponding to new rows of data to be inserted into the relevant table. The
                          order of the data in tuples is important, e.g.
                          save_data = [(x_col_1, x_col_2, ..., x_col_n), ...]
        :param table: Table name in which new data will be inserted.
        """
        cursor = self.get_cursor()
        cols = "col_1, ..., col_n"  # Comma separated list of column names in the exact order of data in save_data
        # Need to generate a string of question marks of same length as number of columns for pyodbc .executemany()
        # insert statement: "INSERT INTO TABLE (col_1, col_2, .., col_n) VALUES (?, ?, ... ?);"
        question_marks = ', '.join(['?'] * len(cols.split(',')))
        sql_cmd = f"INSERT INTO {table} ({cols}) VALUES ({question_marks});"
        # Save in batches to cut down on time
        len_data = len(save_data)
        if 0 < len_data <= 1000:
            cursor.executemany(sql_cmd, save_data)
            cursor.commit()
        elif len_data > 0:
            for k in range(len_data // 1000):
                cursor.executemany(sql_cmd, save_data[1000 * k:1000 * (k + 1)])
                cursor.commit()
            cursor.executemany(sql_cmd, save_data[-(len_data % 1000):])
            cursor.commit()
