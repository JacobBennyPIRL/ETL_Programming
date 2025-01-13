from dagster import job
from extract import *
from transform import *
from visualisation import *
@job
def etl():
         visualise(load(
                join_testing_cases_deaths(join_deaths_cases(
                transform_covid_deaths(
                    extract_covid_deaths()
                )
                ,
                transform_covid_cases(
                    extract_covid_cases()
                )),
                transform_covid_testing(
                extract_covid_testing()
                ))),load_vaccinations(transform_covid_vaccinations(extract_covid_vaccinations())))
 