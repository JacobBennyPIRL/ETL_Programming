import numpy as np
import pandas as pd
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
from sqlalchemy.types import *
from pymongo import MongoClient, errors

# Code to connect to postgreSQL database 
connection_string= "postgresql+psycopg2://dap:dap@127.0.0.1:5432/postgres"

#Code to create new database Covid to load the processed structured data 
try :
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(("CREATE DATABASE covid;"))
 
except exc.SQLAlchemyError as dbError:
    print ("PostgreSQL Error", dbError)
finally :
    if engine in locals():
        engine.close()

#Connection string to connect PostgreSQL and MongoDB
postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/covid"
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()

##################################Code for DataFrame#########################################
#Syntax to create dataframes to load the processed data

#Dataframe for covid testing over time
covidtestingDataFrame= create_dagster_pandas_dataframe_type(
    name="covidtestingDataFrame",
    columns=[
        PandasColumn.datetime_column(
            name="specimen_collection_date",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="tests",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="pos",
            non_nullable=True
        ),
        PandasColumn.float_column(
            name="pct",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="neg",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="indeterminate",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="cumulative_tests",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="cumulative_positive_tests",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="cumulative_negative_tests",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="cumulative_indeterminate_tests",
            non_nullable=True
        ),
        PandasColumn.datetime_column(
            name="data_loaded_at",
            non_nullable=True
        )]
)


#Dataframe for covid deaths over time
coviddeathsDataFrame = create_dagster_pandas_dataframe_type(
    name="coviddeathsDataFrame",
    columns=[
        PandasColumn.datetime_column(
            name="date_of_death",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="new_deaths",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="cumulative_deaths",
            non_nullable=True
        ),
        PandasColumn.datetime_column(
            name="data_as_of",
            non_nullable=True
        ),
        PandasColumn.datetime_column(
            name="data_loaded_at",
            non_nullable=True
        )
    ]
)        

#Dataframe for covid cases over time
covidcasesDataFrame = create_dagster_pandas_dataframe_type(
    name="covidcasesDataFrame",
    columns=[
        PandasColumn.datetime_column(
            name="specimen_collection_date",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="new_cases",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="cumulative_cases",
            non_nullable=True
        ),
        PandasColumn.datetime_column(
            name="data_as_of",
            non_nullable=True
        ),
        PandasColumn.datetime_column(
            name="data_loaded_at",
            non_nullable=True
        )
    ]
) 

#Dataframe for covid vaccinations over time
covidvaccinationsDataFrame = create_dagster_pandas_dataframe_type(
    name="covidvaccinationsDataFrame",
    columns=[
        PandasColumn.datetime_column(
            name="date_administered",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="new_recipients",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="new_2nd_booster_recipients",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="cumulative_recipients",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="cumulative_2nd_booster_recipients",
            non_nullable=True
        )
    ]
)

##################################Code for Transformation#########################################

#Function to transform covid testing data    
@op(
    ins={"start": In(bool)},
    out=Out(covidtestingDataFrame)
)
def transform_covid_testing(start):
    # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    covid_db = client["COVID_DATABASE"]
    # Retrieve the data from the collection, flatten it and
    # return a Pandas data frame
    covid_testing_df = pd.json_normalize(list(covid_db.testing_over_time.find({})))
    coviddeaths_datatypes = dict(
        zip(covid_testing_df.columns, [object]*len(covid_testing_df.columns))
    )
    # Set date column to have the pandas datetime datatype 
    covid_testing_df["specimen_collection_date"] = pd.to_datetime(covid_testing_df["specimen_collection_date"])
    covid_testing_df["data_loaded_at"] = pd.to_datetime(covid_testing_df["data_loaded_at"])
 
    # Convert other columns to numeric
    covid_testing_df["tests"] = pd.to_numeric(covid_testing_df["tests"], errors='coerce').fillna(0).astype(np.int64)
    covid_testing_df["pos"] = pd.to_numeric(covid_testing_df["pos"], errors='coerce').fillna(0).astype(np.int64)
    covid_testing_df["pct"] = pd.to_numeric(covid_testing_df["pct"], errors='coerce').fillna(0).astype(np.float64)
    covid_testing_df["neg"] = pd.to_numeric(covid_testing_df["neg"], errors='coerce').fillna(0).astype(np.int64)
    covid_testing_df["indeterminate"] = pd.to_numeric(covid_testing_df["indeterminate"], errors='coerce').fillna(0).astype(np.int64)
    covid_testing_df["cumulative_tests"] = pd.to_numeric(covid_testing_df["cumulative_tests"], errors='coerce').fillna(0).astype(np.int64)
    covid_testing_df["cumulative_positive_tests"] = pd.to_numeric(covid_testing_df["cumulative_positive_tests"], errors='coerce').fillna(0).astype(np.int64)
    covid_testing_df["cumulative_negative_tests"] = pd.to_numeric(covid_testing_df["cumulative_negative_tests"], errors='coerce').fillna(0).astype(np.int64)
    covid_testing_df["cumulative_indeterminate_tests"] = pd.to_numeric(covid_testing_df["cumulative_indeterminate_tests"], errors='coerce').fillna(0).astype(np.int64)
 
    # Drop the _id column as we don't need it ,"data_loaded_at"
    covid_testing_df.drop(["_id"], axis=1, inplace=True)
 
    # Return the covid testing data frame    
    return covid_testing_df


#Function to transform covid cases data 
@op(
    ins={"start": In(bool)},
    out=Out(covidcasesDataFrame)
)
def transform_covid_cases(start):
    # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    covid_db = client["COVID_DATABASE"]
    # Retrieve the data from the collection, flatten it and
    # return a Pandas data frame
    covid_cases_df = pd.json_normalize(list(covid_db.cases_over_time.find({})))
    coviddeaths_datatypes = dict(
        zip(covid_cases_df.columns, [object]*len(covid_cases_df.columns))
    )
    # Set date column to have the Pandas datetime datatype 
    covid_cases_df["specimen_collection_date"] = pd.to_datetime(covid_cases_df["specimen_collection_date"])
    covid_cases_df["data_as_of"] = pd.to_datetime(covid_cases_df["data_as_of"])
    covid_cases_df["data_loaded_at"] = pd.to_datetime(covid_cases_df["data_loaded_at"])
    
    # Convert other columns to numeric
    covid_cases_df["new_cases"] = pd.to_numeric(covid_cases_df["new_cases"], errors='coerce').fillna(0).astype(np.int64)
    covid_cases_df["cumulative_cases"] = pd.to_numeric(covid_cases_df["cumulative_cases"], errors='coerce').fillna(0).astype(np.int64)
 
    # Drop the _id column as we don't need it ,"data_as_of","data_loaded_at"
    covid_cases_df.drop(["_id"], axis=1, inplace=True)
 
    # Return the covid cases data frame    
    return covid_cases_df


#Function to transform covid deaths data 
@op(
    ins={"start": In(bool)},
    out=Out(coviddeathsDataFrame)
)
def transform_covid_deaths(start):
    # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    covid_db = client["COVID_DATABASE"]
    # Retrieve the data from the collection, flatten it and
    # return a Pandas data frame
    covid_deaths_df = pd.json_normalize(list(covid_db.deaths_over_time.find({})))
    coviddeaths_datatypes = dict(
        zip(covid_deaths_df.columns, [object]*len(covid_deaths_df.columns))
    )
    # Set date column to have the Pandas datetime datatype 
    covid_deaths_df["date_of_death"] = pd.to_datetime(covid_deaths_df["date_of_death"])
    covid_deaths_df["data_as_of"] = pd.to_datetime(covid_deaths_df["data_as_of"])
    covid_deaths_df["data_loaded_at"] = pd.to_datetime(covid_deaths_df["data_loaded_at"])
    
    # Convert other columns to numeric
    covid_deaths_df["new_deaths"] = pd.to_numeric(covid_deaths_df["new_deaths"], errors='coerce').fillna(0).astype(np.int64)
    covid_deaths_df["cumulative_deaths"] = pd.to_numeric(covid_deaths_df["cumulative_deaths"], errors='coerce').fillna(0).astype(np.int64)
 
    # Drop the _id column as we don't need it ,"data_as_of","data_loaded_at"
    covid_deaths_df.drop(["_id"], axis=1, inplace=True)
 
    # Return the covid deaths data frame    
    return covid_deaths_df


    
#Function to transform covid vaccination data
@op(
    ins={"start": In(bool)},
    out=Out(covidvaccinationsDataFrame)
)
def transform_covid_vaccinations(start):
    # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    covid_db = client["COVID_DATABASE"]
    # Retrieve the data from the collection, flatten it and
    # return a Pandas data frame
    covid_vaccinations_df = pd.json_normalize(list(covid_db.vaccinations_over_time_by_DPH_only.find({})))
    covidvaccination_datatypes = dict(
        zip(covid_vaccinations_df.columns, [object]*len(covid_vaccinations_df.columns))
    )
    # Set date column to have the Numpy datetime64 datatype 
    covid_vaccinations_df["date_administered"] = pd.to_datetime(covid_vaccinations_df["date_administered"])
    # Convert "new_deaths" and "cumulative_deaths" columns to integer
    covid_vaccinations_df["new_recipients"] = pd.to_numeric(covid_vaccinations_df["new_recipients"], errors='coerce').fillna(0).astype(np.int64)
    covid_vaccinations_df["new_2nd_booster_recipients"] = pd.to_numeric(covid_vaccinations_df["new_2nd_booster_recipients"], errors='coerce').fillna(0).astype(np.int64)
    covid_vaccinations_df["cumulative_recipients"] = pd.to_numeric(covid_vaccinations_df["cumulative_recipients"], errors='coerce').fillna(0).astype(np.int64)
    covid_vaccinations_df["cumulative_2nd_booster_recipients"] = pd.to_numeric(covid_vaccinations_df["cumulative_2nd_booster_recipients"], errors='coerce').fillna(0).astype(np.int64)    
    # Drop the _id column as we don't need it ,"data_as_of","data_loaded_at"
    covid_vaccinations_df.drop(["_id","administering_provider_type","new_primary_series_doses","new_series_completed","new_booster_doses","new_booster_recipients","new_bivalent_booster_doses","new_bivalent_booster_recipients","cumulative_primary_series_doses","cumulative_series_completed","cumulative_booster_doses","cumulative_booster_recipients","cumulative_bivalent_booster_doses","cumulative_bivalent_booster_recipients","data_as_of","data_updated_at","data_loaded_at"], axis=1, inplace=True)
    
    # Return the covid_vaccinations_df data frame    
    return covid_vaccinations_df



##################################Code for Join#########################################


#Function to join covid deaths and cases data 
   
@op(
    ins={"covid_deaths_df": In(pd.DataFrame), "covid_cases_df": In(covidcasesDataFrame)},
    out=Out(pd.DataFrame)
)

def join_deaths_cases(covid_deaths_df,covid_cases_df) -> pd.DataFrame:
    
    # Join the two data frames
    merged_cases_deaths_df = covid_deaths_df.merge(
        right=covid_cases_df,
        how="inner",
        left_on="date_of_death",
        right_on="specimen_collection_date"
    )
    # Drop the datetime column as we already have a date column
    #merged_cases_deaths_df.drop(["datetime"],axis=1,inplace=True)
    
    # Return the joined data frames
    return(merged_cases_deaths_df)
	

#Function to join covid testing and first join(covid deaths and cases)	
@op(
    ins={"covid_testing_df": In(pd.DataFrame), "merged_cases_deaths_df": In(covidtestingDataFrame)},
    out=Out(pd.DataFrame)
)

def join_testing_cases_deaths(covid_testing_df,merged_cases_deaths_df) -> pd.DataFrame:
    
    # Join the two data frames
    merged_cases_deaths_test_df = merged_cases_deaths_df.merge(
        right=covid_testing_df,
        how="inner",
        left_on="specimen_collection_date",
        right_on="specimen_collection_date"
    )

    # Drop the datetime column as we already have a date column
    #merged_cases_deaths_test_df.drop(["datetime"],axis=1,inplace=True)
    
    # Return the joined data frames
    return(merged_cases_deaths_test_df)
    
##################################Code to Load#########################################
    
#Function to Load the joined covid deaths, cases and testing data     
@op(
    ins={"merged_cases_deaths_test_df": In(pd.DataFrame)},
    out=Out(bool)
)

def load(merged_cases_deaths_test_df):
    try:
        # Create a connection to the PostgreSQL database
        engine = create_engine(
            postgres_connection_string,
            poolclass=NullPool
        )
        # Create a dictionary with column names as the key and the VARCHAR 
        # Create a dictionary with column names as the key and the VARCHAR 
        # type as the value. This will be used to specify data types for the
        # created database. We will change some of these types later.
        database_datatypes = dict(
            zip(merged_cases_deaths_test_df.columns,[VARCHAR]*len(merged_cases_deaths_test_df.columns))
        )
        
        # Set date column to have the TIMESTAMP datatype
        database_datatypes["specimen_collection_date"] = TIMESTAMP
        database_datatypes["data_as_of"] = TIMESTAMP
        database_datatypes["data_loaded_at"] = TIMESTAMP
        database_datatypes["date_of_death"] = TIMESTAMP


        # Set columns with DOUBLE PRECISION datatype
        
        
        # Set columns with INT datatype
        for column in ["new_cases","cumulative_cases","new_deaths","cumulative_deaths","tests","pos","neg","indeterminate","cumulative_tests","cumulative_positive_tests","cumulative_negative_tests","cumulative_indeterminate_tests"]:
            database_datatypes[column] = INT
        
     
        database_datatypes["pct"] = DECIMAL

        # Open the connection to the PostgreSQL server
        with engine.connect() as conn:
            
            # Store the data frame contents to the merged_covid 
            # table, using the dictionary of data types created
            # above and replacing any existing table
            rowcount = merged_cases_deaths_test_df.to_sql(
                name="merged_covid",
                schema="public",
                dtype=database_datatypes,
                con=engine,
                index=False,
                if_exists="replace"
            )
            logger.info("{} records loaded".format(rowcount))
            
        # Close the connection to PostgreSQL and dispose of 
        # the connection engine
        engine.dispose(close=True)
        
        # Return the number of rows inserted
        return rowcount > 0
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


#Function to load the vaccination data
@op(
    ins={"covidvaccinationsDataFrame": In(pd.DataFrame)},
    out=Out(bool)
)

def load_vaccinations(covidvaccinationsDataFrame):
    try:
        # Create a connection to the PostgreSQL database
        engine = create_engine(
            postgres_connection_string,
            poolclass=NullPool
        )
        
        # Create a dictionary with column names as the key and the VARCHAR 
        # type as the value. This will be used to specify data types for the
        # created database. We will change some of these types later.
        database_datatypes = dict(
            zip(covidvaccinationsDataFrame.columns,[VARCHAR]*len(covidvaccinationsDataFrame.columns))
        )
        
        # Set date column to have the TIMESTAMP datatype
        database_datatypes["date_administered"] = TIMESTAMP

        # Set columns with DOUBLE PRECISION datatype
        
        
        # Set columns with INT datatype
        for column in ["new_recipients","new_2nd_booster_recipients","cumulative_recipients","cumulative_2nd_booster_recipients"]:
            database_datatypes[column] = INT
        
     
            
        # Open the connection to the PostgreSQL server
        with engine.connect() as conn:
            
            # Store the data frame contents to the vaccinations_over_time 
            # table, using the dictionary of data types created
            # above and replacing any existing table
            rowcount = covidvaccinationsDataFrame.to_sql(
                name="vaccinations_over_time",
                schema="public",
                dtype=database_datatypes,
                con=engine,
                index=False,
                if_exists="replace"
            )
            logger.info("{} records loaded".format(rowcount))
            
        # Close the connection to PostgreSQL and dispose of 
        # the connection engine
        engine.dispose(close=True)
         
        # Return the number of rows inserted
        return rowcount > 0
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False

