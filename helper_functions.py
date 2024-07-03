import pandas as pd
import re

def camel_to_snake(col_name):
    # Convert camelcase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return snake_case

def convert_column_name(col_name):
    # Convert camelcase to snake_case
    col_snake_case = camel_to_snake(col_name)
    # Convert to uppercase
    col_upper = col_snake_case.upper()
    # Add underscore if column name starts with a number
    if col_upper[0].isdigit():
        col_upper = '_' + col_upper
    return col_upper

def generate_feature_descriptions(data_description_file: str):
    # Define the list to store the matching lines
    features = []
    descriptions = []
    
    # Open the file in read mode
    with open(data_description_file, "r") as file:
        # Iterate through each line in the file
        for line in file:
            # Check if the line matches the pattern: word followed by a colon
            if line.strip() and ':' in line:
                feature = line.split(':', 1)[0]
                if feature.isalnum() and line.strip().startswith(feature + ':'):
                    # Add the line to the list (strip to remove newline characters)
                    features.append(convert_column_name(feature))
                    descriptions.append(line.split(':', 1)[1].strip())
    feature_descriptions = dict(zip(features, descriptions))
    return feature_descriptions