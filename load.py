import sqlite3


class MetadataLoader:
    '''creates a table in the database by trasnferring the metadata
    dataframe. Indexes are added for faster queries.
    '''

    def __init__(self, database_file_path):
        self.database_file_path = database_file_path

    def transfer(self, metadata_df, name):
        with sqlite3.connect(self.database_file_path) as conn:
            metadata_df.to_sql(f'{name}', conn, if_exists='replace', index=False)
            conn.commit()
            print('Table sucessfully created from df!')

    def add_indexes(self, table):
        with sqlite3.connect(self.database_file_path) as conn:
            cur = conn.cursor()
            if table == 'Email':
                cur.execute('''
                    CREATE INDEX idxCol1 ON Email ("From")
                ''')

                cur.execute('''
                    CREATE INDEX idxCol2 ON Email (Attachments)
                ''')

                conn.commit()
                print('Indexes created!')

            else:
                raise ValueError(f'Table name with {table} does not exists!')
