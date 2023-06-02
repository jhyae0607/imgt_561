import os
import extract
import transform
import openai
from tqdm import tqdm
import pandas as pd

# init openai info
openai.api_key = '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'
model_id = 'gpt-3.5-turbo'

#---------------------------------------------------------------------
# Extraction Logics
url_csv ='https://storage.googleapis.com/kaggle-data-sets/55/120/compressed/emails.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230529%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230529T221602Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=0bb7400d1f4f44b3fbbc60e55337efc6f6fb6e163aed50fed838fdac8c973703df4c8db9cc727fc738ecc1f92efd8f19a61f8711fcaffebe72dd90880ee4fd02db96122b2e0fee95a2cd42e7c13311ace7909f9570eb1b1faee95e34748370e295ca35b2715b173c2beb52c00c21a82295d862621baff0b486ec74435ccbccd7cb8ed97306a0c6b090f89d96ee5734ddfecc1c3d1c8ebf40f1c2f32c48869f52eb1f2a9fefa1a6e0ba5b89ab1f5e8d69da01c499cb4dc2d2f83296fbcd53e856159b1598e99b837c88b43629f7bf4c04718f6dacfaaf5f3784392827601fe42f77e9a1ed842d42d24755cf446e825f875a14b45444bc67cf3171332be636bde2'
metadata_extractor = extract.MetadataExtractor(url_csv)

# init input parameters
metadata_lst = []
num_samples = 10
random_state = 7
file_path_email = './saved_df/email_df'
file_path_metadata = './saved_metadata/metadata_lst'

# Retrieve dataset from existing file
if os.path.exists(file_path_email):
    print("Dataframe already exists!")
    # load dataframe
    email_df = metadata_extractor.load_data(file_path_email)

else:
    print("Dataframe does not exists!")
    # retrieve/random sample dataframe from url
    extracted_df = metadata_extractor.extract_url()
    email_df = metadata_extractor.random_sample(extracted_df, num_samples, random_state)
    metadata_extractor.save_data(email_df, file_path_email)

# Extract metadata from existing file
if os.path.exists(file_path_metadata):
    print("Metadata already exists!")
    metadata_lst = metadata_extractor.load_data(file_path_metadata)

else:
    print("Metadata does not exists!")
    # extract metadata from ChatGPT
    for email in tqdm(email_df['message'], desc='Extracting Metadata'):
        metadata = metadata_extractor.extract_metadata(email)
        metadata_lst.append(metadata)
    metadata_extractor.save_data(metadata_lst, file_path_metadata)

#---------------------------------------------------------------------
# Transformation Logics
metadata_cols = ['MessageID', 'Date', 'From', 'To', 'Cc', 'Bcc', 'Subject', 'MimeVersion',
                 'ContentType', 'ContentTransferEncoding', 'Summarized Content', 'Attachments']
metadata_df = pd.DataFrame(columns=metadata_cols)
metadata_transformer = transform.MetadataTransformer(metadata_lst, metadata_df)

# convert list of json objects to pandas dataframe
metadata_df = metadata_transformer.convert_to_df()

# convert date column from PST to EST
metadata_df = metadata_transformer.convert_datetime()

# replace null values with "None"
metadata_df = metadata_transformer.replace_nulls()

# remove punctuations
metadata_df = metadata_transformer.remove_punctuations()
#---------------------------------------------------------------------

print(metadata_df.head())