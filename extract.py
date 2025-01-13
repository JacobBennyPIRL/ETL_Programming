import json
import pandas as pd
from dagster import op, Out, In, get_dagster_logger
from datetime import date, datetime
from pymongo import MongoClient, errors
from pprint import pprint
import urllib.request
import ssl
import xml.etree.ElementTree as ET
import hashlib
ssl._create_default_https_context = ssl._create_unverified_context

#Code to connect to mongoDB abd create COVID_DATABASE
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()
client = MongoClient(mongo_connection_string)
covid_db = client["COVID_DATABASE"]

#Function to extract covid_cases using API
@op(
    out=Out(bool)
)

def extract_covid_cases() -> bool:
    result = True
    try:
        # Connect to the MongoDB database
        cases_collection = covid_db["cases_over_time"]
        #API URL
        case_url="https://data.sfgov.org/resource/gyr2-k29z.json?$limit=2000&$offset=0"

        # Open the API containing the data
        with urllib.request.urlopen(case_url) as response:
            cases_json = json.loads(response.read())
            
        for cases in cases_json:
            try:
                # Create a key for the MongoDB collection. This 
                # ensures that we cannot have duplicate documents
                key="{}".format(
                cases["specimen_collection_date"]
                )
                cases["_id"] = key
                    
                # Insert the cases data as a document in the cases_collection 
                cases_collection.insert_one(cases)
                    
                # Trap and handle duplicate key errors
            except errors.DuplicateKeyError as err:
                logger.error("Error: %s" % err)
                continue
            
    # Trap and handle other errors
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    # Return a Boolean indicating success or failure
    return result
   
#Function to extract covid_testing over the time using XML    
@op(
    out=Out(bool)
)

def extract_covid_testing() -> bool:
    result = True
    try:
        # Connect to the testing over time collection in MongoDB
        testing_collection = covid_db["testing_over_time"]
    

        # Open the file containing the data       
        try:
            file=open("1378_Testing_Over_Time.xml",'r',encoding='utf-8')
            testing_xml=ET.fromstring(file.read())
            
            # catch the specific exceptions while accessing a file and throwing a userdefined error message
        except PermissionError:
            print('Permission denied. please check permission for the path {}'.format(fileName))       
        except IsADirectoryError:
            print('check the directory{}'.format(fileName))
        except FileNotFoundError:
            print('Could not open the file in path {}'.format(fileName))
            # catch broader exception for any errors that is unhandled
        except Exception as ex:
            print('Unexpected error in opening the file {}'.format(repr(ex)))
            # closing the connection to free up the memory
        finally:
            if 'file' in locals():
                file.close()
            
        for x in testing_xml:
            for y in x:
                key=y.find("specimen_collection_date").text
                specimen_collection_date=y.find("specimen_collection_date").text
                tests=y.find("tests").text
                pos=y.find("pos").text
                pct=y.find("pct").text
                neg=y.find("neg").text
                indeterminate=y.find("indeterminate").text
                cumulative_tests=y.find("cumulative_tests").text
                cumulative_positive_tests=y.find("cumulative_positive_tests").text
                cumulative_negative_tests=y.find("cumulative_negative_tests").text
                cumulative_indeterminate_tests=y.find("cumulative_indeterminate_tests").text
                data_loaded_at=y.find("data_loaded_at").text
                stu_dict={'_id':key,'specimen_collection_date': specimen_collection_date,'tests':tests,'pos':pos,'pct':pct,'neg':neg,'indeterminate':indeterminate,'cumulative_tests':cumulative_tests,'cumulative_positive_tests':cumulative_positive_tests,'cumulative_negative_tests':cumulative_negative_tests,'cumulative_indeterminate_tests':cumulative_indeterminate_tests,'data_loaded_at':data_loaded_at}
                testing_collection.insert_one(stu_dict)
                            
                # Trap and handle duplicate key errors
    except errors.DuplicateKeyError as err:
        logger.error("Error: %s" % err)
            
            
    # Trap and handle other errors
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    # Return a Boolean indicating success or failure
    return result
    
#Function to extract covid_deaths using JSON  
@op(
    out=Out(bool)
)

def extract_covid_deaths() -> bool:
    result = True
    try:
        # Connect to the deaths over time collection in Mongodb
        deaths_collection = covid_db["deaths_over_time"]
    

        # Open the file containing the data
        
        with open("1359_Deaths_Over_Time.json","r") as fh:
        
            # Load the JSON data from the file
            data = json.load(fh)
        try:
            for x in data:
                key="{}".format(
                x["date_of_death"]
                )
                x["_id"] = key
                deaths_collection.insert_one(x)

        except errors.DuplicateKeyError as err:
            logger.error("Error: %s" % err)
            
            
    # Trap and handle other errors
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    # Return a Boolean indicating success or failure
    return result
    
#Function to extract covid_vaccinations using API    

@op(
    out=Out(bool)
)

def extract_covid_vaccinations() -> bool:
    result = True
    try:
        vaccinations_url="https://data.sfgov.org/resource/rutu-rpar.json?$limit=2500&$offset=0"
               
        with urllib.request.urlopen(vaccinations_url) as response:
            vaccinations_json = json.loads(response.read())
               
        for vaccination in vaccinations_json:
            if vaccination["administering_provider_type"]=="DPH Only":
                try:
                    vaccinations_providers_collection = covid_db["vaccinations_over_time_by_DPH_only"]
        
                        # Create a key for the MongoDB collection. This 
                        # ensures that we cannot have duplicate documents
                    key="{}".format(
                    vaccination["date_administered"]
                    )
                    vaccination["_id"] = key
                            
                            # Insert the vaccination data as a document in the vaccination over time collection 
                    vaccinations_providers_collection.insert_one(vaccination)
                            
                        # Trap and handle duplicate key errors
                except errors.DuplicateKeyError as err:
                    logger.error("Error: %s" % err)
                    continue
            
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False                         

    
    # Return a Boolean indicating success or failure
    return result