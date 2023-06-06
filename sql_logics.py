import sqlite3


class Database:

    def __init__(self, database_file_path):
        self.database_file_path = database_file_path

    # insert logic
    def insert(self, metadata_row, table_name):
        with sqlite3.connect(self.database_file_path) as conn:
            cur = conn.cursor()
            cur.execute(f'''
                INSERT INTO {table_name} (MessageID, Date, "From", "To", Cc, Bcc, Subject, MimeVersion, ContentType, ContentTransferEncoding, "Summarized Content", Attachments)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (metadata_row))

            conn.commit()
            print('Df column successfully inserted!')

    # update logic
    def update(self, set_col, where_col, set_data, where_data, table_name):
        with sqlite3.connect(self.database_file_path) as conn:
            cur = conn.cursor()
            cur.execute(f'''
            UPDATE {table_name} SET {set_col} = ? WHERE {where_col} = ?
            ''', (set_data, where_data))

            conn.commit()
            print('Successfully Updated!')

    # delete logic
    def delete(self, where_col, where_data, table_name):
        with sqlite3.connect(self.database_file_path) as conn:
            cur = conn.cursor()
            cur.execute(f'''
            DELETE FROM {table_name} WHERE {where_col} = ?
            ''', (where_data,))

            conn.commit()
            print('Successfully Deleted!')

    # retrieve single logic
    def retrieve_single(self, select_col, where_col, where_data, table_name):
        with sqlite3.connect(self.database_file_path) as conn:
            cur = conn.cursor()
            cur.execute(f'''
                SELECT {select_col} FROM {table_name} WHERE {where_col} = ?
            ''', (where_data,))
            row = cur.fetchone()

        if row is None:
            return row
        else:
            print(f'Retrieve_single data: {row[0]}')
            return row[0]

    # retrieve many logic
    def retrieve_many(self, select_col, where_col, where_data, table_name):
        with sqlite3.connect(self.database_file_path) as conn:
            cur = conn.cursor()
            cur.execute(f'''
                SELECT {select_col} FROM {table_name} WHERE {where_col} = ?
            ''', (where_data,))
            row = cur.fetchall()

        print(f'Retrive_many data: {row}')
        return row