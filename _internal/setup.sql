USE ROLE ACCOUNTADMIN;

-- Create warehouses
CREATE WAREHOUSE IF NOT EXISTS TRAIN_WH WITH WAREHOUSE_SIZE='MEDIUM';
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE='X-SMALL';

-- Setup Procedure
WITH SETUP AS PROCEDURE()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python','snowflake-ml-python==1.5.1','snowflake.core==0.8.0')
  IMPORTS = ('@KAGGLE_TITANIC_CHALLENGE.PUBLIC.TITANIC_CHALLENGE_REPO/branches/main/_internal/data/train.csv',
             '@KAGGLE_TITANIC_CHALLENGE.PUBLIC.TITANIC_CHALLENGE_REPO/branches/main/_internal/data/test.csv',
             '@KAGGLE_TITANIC_CHALLENGE.PUBLIC.TITANIC_CHALLENGE_REPO/branches/main/_internal/data/helper_functions.py',
             '@KAGGLE_TITANIC_CHALLENGE.PUBLIC.TITANIC_CHALLENGE_REPO/branches/main/_internal/data/feature_descriptions.json')
  HANDLER = 'run'
  AS
$$
def run(session):
    import pandas as pd
    import sys
    import json
    import snowflake.snowpark.functions as F
    from snowflake.snowpark.functions import col, lit, when
    from snowflake.core import Root
    from snowflake.core._common import CreateMode
    from snowflake.core.schema import Schema
    from snowflake.core.warehouse import Warehouse
    from snowflake.ml.feature_store import FeatureStore, FeatureView, Entity, CreationMode
    from helper_functions import convert_column_name

    IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
    import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
    
    # Create a fresh Schema for this challenge
    root = Root(session)
    ml_demo_db = root.databases["KAGGLE_TITANIC_CHALLENGE"]
    ml_demo_schema = Schema(name="DEVELOPMENT")
    ml_demo_schema = ml_demo_db.schemas.create(ml_demo_schema, mode='or_replace')
    
    # Set context
    session.use_schema('KAGGLE_TITANIC_CHALLENGE.DEVELOPMENT')
    
    # Create Feature Store
    fs = FeatureStore(
        session=session, 
        database="KAGGLE_TITANIC_CHALLENGE", 
        name="DEVELOPMENT", 
        default_warehouse="COMPUTE_WH",
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    # Load data 
    train_df = pd.read_csv(import_dir + 'train.csv')
    test_df = pd.read_csv(import_dir + 'test.csv')
    
    # Combine train and test data
    df = pd.concat([train_df,test_df])
    df.columns = [convert_column_name(col_name) for col_name in df.columns]
    passenger_features = df.drop('SURVIVED', axis=1)
    passenger_labels = df[['PASSENGER_ID','SURVIVED']]

    # Persist data
    passenger_features = session.write_pandas(passenger_features, table_name='PASSENGER_FEATURES', overwrite=True, quote_identifiers=False)
    passenger_labels = session.write_pandas(passenger_labels, table_name='PASSENGER', overwrite=True, quote_identifiers=False)

    # Convenience Features
    # Uppercase the SEX column (will be useful when we one-hot-encode this variable)
    passenger_features = passenger_features.with_column('SEX', F.upper(col('SEX')))
    
    # Add the names for embarkation and uppercase them
    # Southampton (S), Cherbourg (C), Queenstown (Q)
    passenger_features = passenger_features.with_column('EMBARKED', 
        when(col('EMBARKED')=='S', lit('Southampton'))
        .when(col('EMBARKED')=='C', lit('Cherbourg'))
        .when(col('EMBARKED')=='Q', lit('Queenstown')))
    passenger_features = passenger_features.with_column('EMBARKED', F.upper(col('EMBARKED')))

    # Read feature descriptions
    with open(import_dir + 'feature_descriptions.json', 'r') as file:
        feature_descriptions = json.load(file)

    # Create a new entity for the Feature Store
    entity = Entity(name="PASSENGER", join_keys=["PASSENGER_ID"], desc='Unique identifier for passengers.')
    fs.register_entity(entity)
    
    # Create Feature View with Kaggle Features
    kaggle_fv = FeatureView(
        name="PASSENGER_KAGGLE_FEATURES", 
        entities=[entity],
        feature_df=passenger_features, 
        refresh_freq="1 minute",
        desc="Passenger Features from Kaggle")
    
    # Add descriptions for features
    kaggle_fv = kaggle_fv.attach_feature_desc(feature_descriptions)

    # Register the Feature View
    kaggle_fv = fs.register_feature_view(
        feature_view=kaggle_fv, 
        version="V1", 
        block=True)

    # Create stage for submission files
    session.sql("""
    CREATE OR REPLACE STAGE KAGGLE_SUBMISSION
      ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
      DIRECTORY = (
        ENABLE = true
      )
      """).collect()
    return "SUCCESS"
$$
CALL SETUP();

CREATE OR REPLACE PROCEDURE calculate_challenge_score(STAGE_PATH STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','snowflake-ml-python==1.5.1')
IMPORTS = ('@KAGGLE_TITANIC_CHALLENGE.PUBLIC.TITANIC_CHALLENGE_REPO/branches/main/_internal/data/test_100_percent.csv')
HANDLER = 'calculate_score'
AS
$$
import sys
import pandas as pd
from snowflake.ml.modeling.metrics import accuracy_score
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

def calculate_score(session, stage_path):
    try:
        truth = session.create_dataframe(pd.read_csv(import_dir + 'test_100_percent.csv'))
        kaggle_submission = session.read.option("infer_schema", True).option("PARSE_HEADER", True).csv(stage_path)
        evaluation_df = truth.join(kaggle_submission, on='"PassengerId"', rsuffix='_PROVIDED')
        accuracy = accuracy_score(df=evaluation_df, y_true_col_names='"Survived"', y_pred_col_names='"Survived_PROVIDED"')
        return f'Congrats! Your Accuracy is: {accuracy}'
    except Exception as e:
        return e
$$;

SHOW TABLES;