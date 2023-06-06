import os
import extract as extract
import transform as transform
import load as load
import openai
from tqdm import tqdm
import pandas as pd
import sql_logics as sql_logics


# change cwd
# print(os.getcwd())
os.chdir('/home/jung/Documents/data_science/23_SP/db_eeng/project/imgt_561')

# openai info
openai.api_key = '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'
model_id = 'gpt-3.5-turbo'

'''''
Extraction Logic Examples
'''''
# init input parameters
url_csv = 'https://storage.googleapis.com/kaggle-data-sets/55/120/compressed/emails.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230602%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230602T061725Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=0f0970f1bdd6fc8f602bdefdd3d2e918910ce8194816b54f975edd4cc1e3ceb7a15c716d93edca107a0e3d8c06583560da91238c876ecdd079a976bcffac99233c1ff973aecc57b39d67a1a115b61d265cd14695d07db0398e660b9447205f2b28411a4dde0c2ec11945e6551aca7ec0c8022917b3070d7ee02cf2e22bd4144626a7d4dfebeacccfa64ce403a896329f2c09f76031c845f1989afdcdb9ee74310ea68806812c6be4d85eeeb9c1df29f28bf76331cee63d1f89b6c662a449f726ab1e15b62237346fb98ac753748f368bc74e98c66777ffe784cfe0c8c91738d2da5e46a925b493e9daef0cc4305d7006289683f773c424853ccba4f655809a21'
metadata_extractor = extract.MetadataExtractor(url_csv)
metadata_lst = []
num_samples = 10
random_state = 42
file_path_email = './saved_df/email_df'
file_path_metadata = './saved_metadata/metadata_lst'

# Retrieve dataset from existing file
if os.path.exists(file_path_email):
    print("Dataframe already exists!")
    # load dataframe
    email_df = metadata_extractor.load_data(file_path_email)

else:
    print("Dataframe does not exists!")
    # Retrieve/random sample dataframe from url
    extracted_df = metadata_extractor.extract_url()
    email_df = metadata_extractor.random_sample(extracted_df, num_samples,
                                                random_state)
    metadata_extractor.save_data(email_df, file_path_email)

# Retrieve metadata from existing file
if os.path.exists(file_path_metadata):
    print("Metadata already exists!")
    metadata_lst = metadata_extractor.load_data(file_path_metadata)

else:
    print("Metadata does not exists!")
    # Extract metadata from ChatGPT
    for email in tqdm(email_df['message'], desc='Extracting Metadata'):
        metadata = metadata_extractor.extract_metadata(email)
        metadata_lst.append(metadata)
    metadata_extractor.save_data(metadata_lst, file_path_metadata)


'''''
Transformation Logic Execution Examples
'''''
metadata_cols = ['MessageID', 'Date', 'From', 'To', 'Cc', 'Bcc', 'Subject',
                 'MimeVersion', 'ContentType', 'ContentTransferEncoding',
                 'Summarized Content', 'Attachments']
metadata_df = pd.DataFrame(columns=metadata_cols)
metadata_transformer = transform.MetadataTransformer(metadata_lst, metadata_df)

# convert list of json objects to pandas dataframe
metadata_df = metadata_transformer.convert_to_df()

# convert date column from PST to EST
metadata_df = metadata_transformer.convert_datetime()

# replace null values with "None"
metadata_df = metadata_transformer.replace_nulls()

# remove special characters
metadata_df = metadata_transformer.remove_punctuations()


'''''
Loading Logic Execution Examples
'''''
file_path_db = './email_db/email.db'
table_name = 'Email'
metadata_loader = load.MetadataLoader(file_path_db)

# create sql table from df
metadata_loader.transfer(metadata_df, table_name)

# add indexes
metadata_loader.add_indexes(table_name)


'''''
SQL Command Execution Examples
'''''
db = sql_logics.Database(file_path_db)

# insert logic
db.insert(metadata_df.iloc[7], "Email")

# update logic
db.update("Bcc", "MessageID", "jung.yae@afit.edu",
          "<21013688.1075844564560.JavaMail.evans@thyme>",
          "Email")

# delete logic
db.delete("Bcc", "None", "Email")

# retrieve single logic
db.retrieve_single("Subject", "Bcc", "jung.yae@afit.edu", "Email")
db.retrieve_single("Subject", '"From"', 'susan.scott@enron.com', "Email")

# retrieve many logic
db.retrieve_many("Subject", "Attachments", "None", "Email")

# the created sqlite database is saved under './email_db' folder
