import unittest
import pandas as pd

from extract import MetadataExtractor
from transform import MetadataTransformer
from sql_logics import Database
from load import MetadataLoader


class TransformerTest(unittest.TestCase):

    def set_up(self):
        url_csv = 'https://storage.googleapis.com/kaggle-data-sets/55/120/compressed/emails.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230602%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230602T061725Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=0f0970f1bdd6fc8f602bdefdd3d2e918910ce8194816b54f975edd4cc1e3ceb7a15c716d93edca107a0e3d8c06583560da91238c876ecdd079a976bcffac99233c1ff973aecc57b39d67a1a115b61d265cd14695d07db0398e660b9447205f2b28411a4dde0c2ec11945e6551aca7ec0c8022917b3070d7ee02cf2e22bd4144626a7d4dfebeacccfa64ce403a896329f2c09f76031c845f1989afdcdb9ee74310ea68806812c6be4d85eeeb9c1df29f28bf76331cee63d1f89b6c662a449f726ab1e15b62237346fb98ac753748f368bc74e98c66777ffe784cfe0c8c91738d2da5e46a925b493e9daef0cc4305d7006289683f773c424853ccba4f655809a21'
        file_path_metadata = './saved_metadata/metadata_lst'
        self.metadata_cols = ['MessageID', 'Date', 'From', 'To', 'Cc', 'Bcc', 'Subject',
                              'MimeVersion', 'ContentType', 'ContentTransferEncoding',
                              'Summarized Content', 'Attachments']

        # load the extracted metadata and convert json object to pandas df
        metadata_extractor = MetadataExtractor(url_csv)
        self.metadata_lst = metadata_extractor.load_data(file_path_metadata)
        metadata_df = pd.DataFrame(columns=self.metadata_cols)
        metadata_transformer = MetadataTransformer(self.metadata_lst, metadata_df)
        self.metadata_df = metadata_transformer.convert_to_df()
        self.metadata_df = metadata_transformer.convert_datetime()
        self.metadata_df = metadata_transformer.replace_nulls()
        self.metadata_df = metadata_transformer.remove_punctuations()
        return self.metadata_df

    def test_datatype(self):
        # checks if metadata has been converted from json object to pandas df
        self.metadata_df = TransformerTest.set_up(self)
        self.assertIs(type(self.metadata_df), pd.core.frame.DataFrame)

    def test_column_match(self):
        # checks to see if all columns match
        self.metadata_df = TransformerTest.set_up(self)
        self.assertEqual(len(self.metadata_cols), self.metadata_df.shape[1])
        self.assertEqual(self.metadata_cols, list(self.metadata_df.columns))

    def test_row_match(self):
        # checks if number of rows match
        self.metadata_df = TransformerTest.set_up(self)
        self.assertEqual(len(self.metadata_lst), self.metadata_df.shape[0])

    def test_datetime(self):
        # checks if metadata has a specific datetime format
        self.metadata_df = TransformerTest.set_up(self)
        expected_format = '%Y-%m-%d %H:%M:%S'
        for dt in self.metadata_df['Date']:
            with self.subTest(dt=dt):
                self.assertEqual(dt, pd.to_datetime(dt).strftime(expected_format))

    def test_null(self):
        # checks if there are any null values
        self.metadata_df = TransformerTest.set_up(self)
        null_values = self.metadata_df.isnull().values.any()
        self.assertFalse(null_values)


class SqlQueryTest(unittest.TestCase):

    def set_up(self):
        url_csv = 'https://storage.googleapis.com/kaggle-data-sets/55/120/compressed/emails.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230602%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230602T061725Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=0f0970f1bdd6fc8f602bdefdd3d2e918910ce8194816b54f975edd4cc1e3ceb7a15c716d93edca107a0e3d8c06583560da91238c876ecdd079a976bcffac99233c1ff973aecc57b39d67a1a115b61d265cd14695d07db0398e660b9447205f2b28411a4dde0c2ec11945e6551aca7ec0c8022917b3070d7ee02cf2e22bd4144626a7d4dfebeacccfa64ce403a896329f2c09f76031c845f1989afdcdb9ee74310ea68806812c6be4d85eeeb9c1df29f28bf76331cee63d1f89b6c662a449f726ab1e15b62237346fb98ac753748f368bc74e98c66777ffe784cfe0c8c91738d2da5e46a925b493e9daef0cc4305d7006289683f773c424853ccba4f655809a21'
        file_path_metadata = './saved_metadata/metadata_lst'
        self.metadata_cols = ['MessageID', 'Date', 'From', 'To', 'Cc', 'Bcc', 'Subject',
                              'MimeVersion', 'ContentType', 'ContentTransferEncoding',
                              'Summarized Content', 'Attachments']
        self.file_path_db = './email_db/test.db'
        self.table_name = 'Test'

        # load the extracted metadata and convert json object to pandas df
        metadata_extractor = MetadataExtractor(url_csv)
        self.metadata_lst = metadata_extractor.load_data(file_path_metadata)
        metadata_df = pd.DataFrame(columns=self.metadata_cols)
        metadata_transformer = MetadataTransformer(self.metadata_lst, metadata_df)
        self.metadata_df = metadata_transformer.convert_to_df()
        self.metadata_df = metadata_transformer.convert_datetime()
        self.metadata_df = metadata_transformer.replace_nulls()
        self.metadata_df = metadata_transformer.remove_punctuations()

        # create sql table from pandas df
        metadata_loader = MetadataLoader(self.file_path_db)
        self.db_instance = metadata_loader.transfer(self.metadata_df, self.table_name)

    def test_insert(self):
        # inserts test data and checks assertEqual using retrieve_single data
        SqlQueryTest.set_up(self)
        db = Database(self.file_path_db)
        self.data = ['testMessageID', '2023-06-05 00:00:00', 'jung.yae@afit.edu', 'yae.jung@afit.edu', 'None', 'None', 'TestSubject', '1.0', 'TestContentType', '7bit', 'TestSummary', 'None']
        subject = 'TestSubject'
        db.insert(self.data, self.table_name)
        data_from_database = db.retrieve_single("Subject", '"From"', 'jung.yae@afit.edu', self.table_name)
        self.assertEqual(subject, data_from_database)
        pass

    def test_delete(self):
        # deletes all the rows with 'None' Attachments and validate if returned value is None
        SqlQueryTest.set_up(self)
        db = Database(self.file_path_db)
        db.delete("Attachments", "None", self.table_name)
        data_from_database = db.retrieve_single("Subject", 'Attachments', 'None', self.table_name)
        self.assertIsNone(data_from_database)
        pass

    def test_update(self):
        # updates the subject to 'TestSubject' for rows with a specific 'To' value
        # and checks using assertEqual
        SqlQueryTest.set_up(self)
        db = Database(self.file_path_db)
        db.update('Subject', '"To"', 'TestSubject', 'dgiron@enron.com', self.table_name) 
        subject = 'TestSubject'
        data_from_database = db.retrieve_single('Subject', '"To"', 'dgiron@enron.com', self.table_name)
        self.assertEqual(subject, data_from_database)
        pass


if __name__ == '__main__':
    unittest.main()
