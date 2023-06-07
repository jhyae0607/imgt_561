import io
import time
import openai
import pickle
import zipfile
import requests
import pandas as pd


class MetadataExtractor:
    '''Loads zipped csv from URL, unzips/converts to pandas df
    randomly subsets the df, extracts metadata using gpt-3.5,
    saves extracted metadata into json format.
    The .csv file must contain all the raw textual email data in
    a single column named "message".
    '''

    def __init__(self, url_csv):
        self.url_csv = url_csv

    def extract_url(self):
        response = requests.get(self.url_csv)
        print(f"Status Code {response.status_code}")

        if response.status_code == 200:

            with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
                zipped_csv = zip_ref.namelist()[0]
                unzipped_csv = zip_ref.read(zipped_csv)

            response_content = unzipped_csv
            self.csv_df = pd.read_csv(io.BytesIO(response_content))
            # print('Dataframe created from URL')

        else:
            print('Bad')
            raise Exception('Invalid Response')

        return self.csv_df

    def random_sample(self, df, num_samples, random_state):
        self.sampled_df = df.sample(n=num_samples, random_state=random_state)
        return self.sampled_df

    def extract_metadata(self, message):
        retries = 3
        self.metadata = None

        while retries > 0:
            message = [
                {"role": "user",
                 "content": f"Extract the following metadata in a json format (MessageID, Date, From, To, Cc, Bcc, Subject, MimeVersion, ContentType, ContentTransferEncoding, Summarized Content, Attachments) from the text below. The summarized content should be less than 4 sentences: {message}"
                 }
            ]

            completion = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=message,
                temperature=0
            )

            response_text = completion.choices[0].message.content
            # print(response_text)

            # check if retrieved valid response by length
            if len(response_text) > 0:
                self.metadata = response_text
                break
            else:
                retries -= 1
                time.sleep(60)

        retries = 3
        time.sleep(60)
        return self.metadata

    def save_data(self, data, filepath):
        # save list of json files for later use
        with open(filepath, 'wb') as file:
            pickle.dump(data, file)

    def load_data(self, filepath):
        # load list of json files
        with open(filepath, 'rb') as file:
            metadata_lst = pickle.load(file)

        return metadata_lst
