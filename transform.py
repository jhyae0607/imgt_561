import pandas as pd
import json
import datetime
import re


class MetadataTransformer:
    '''takes list of string values of json objects as input,
    converts the input to pandas dataframe,
    converts datetime data to PDT to EST,
    replaces empty or '[]' data to 'None',
    returns the cleaned data as pandas dataframe'''

    def __init__(self, json_lst, metadata_df):
        self.json_lst = json_lst
        self.metadata_df = metadata_df

    def convert_to_df(self):
        for element in self.json_lst:
            metadata_json = json.loads(element)
            new_row = pd.DataFrame([metadata_json])
            self.metadata_df = pd.concat([self.metadata_df, new_row],
                                         ignore_index=True)

        return self.metadata_df

    def convert_datetime(self):
        datetime_lst = []

        for date in self.metadata_df['Date']:
            # modify date to "%Y-%m-%d %H:%M:%S" format
            input_datetime = str(date)
            input_datetime = input_datetime[:-12]
            dt_object = datetime.datetime.strptime(input_datetime,
                                                   "%a, %d %b %Y %H:%M:%S")
            output_datetime = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            output_datetime = datetime.datetime.strptime(output_datetime,
                                                         "%Y-%m-%d %H:%M:%S")

            # convert from pst to est
            time_diff = datetime.timedelta(hours=3)
            est_datetime = output_datetime - time_diff
            est_datetime = est_datetime.strftime("%Y-%m-%d %H:%M:%S")

            # append to date_lst
            datetime_lst.append(est_datetime)

        # replace date with modified datetime format
        self.metadata_df['Date'] = datetime_lst

        return self.metadata_df

    def replace_nulls(self):
        self.metadata_df.replace([''], 'None', inplace=True)
        self.metadata_df = self.metadata_df.applymap(lambda x: re.sub(r'^\[\s*\]$', 'None', str(x)))

        return self.metadata_df
