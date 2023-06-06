# imgt_561 Final Project
## I performed the database project using metadata extracted from raw email textual data. Below describes what each .py file is intended to do.

### extract.py
#### As an initial step in the ETL pipeline, this code contains functions:
#### - that extracts .csv file from a url and converts it to a pandas dataframe.
#### - that selects random samples from the collected .csv file.
#### - that extract metadata from raw email text data using ChatGPT API
#### - that saves/loads any inputted data into a pickle file

### transform.py
#### To transform and standardize the extracted metadata, this code contains functions:
#### - that converts a list of json objects to a pandas dataframe
#### - that standardizes datetime into specific format and converts the time from PST to EST
#### - that replaces all null values to "None"

### load.py
#### Once the metadata has been cleaned, this code contains functions:
#### - that transfers the cleaned dataframe into a sqlite table
#### - that adds indexes for faster queries

### sql_logics.py
#### -This code contains basic sql commands that performs insert, update, delete, and select single/many

### unit_test.py
#### -This code conducts unit tests on the Transform logic and the sql-command logics

### script.py
#### -This code executes all the functions created from the above codes.
