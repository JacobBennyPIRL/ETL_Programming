import pandas.io.sql as sqlio
import psycopg2
from dagster import op, In
from bokeh.plotting import figure, show
from sqlalchemy import create_engine, event, text, exc
from sqlalchemy.engine.url import URL
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.palettes import Spectral11
from sqlalchemy import create_engine, text
import pandas as pd
from markupsafe import Markup
from bokeh.io import export_png

#Function to create Time Series plot for Deaths Over Time
@op(
    ins={"start": In(bool),"start2": In(bool)}
)

def visualise(start,start2):
    
    postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/covid"

    # A query to return the number of minutes delay in departures and the rainfall
    query = """
    SELECT date_of_death, new_deaths, cumulative_deaths FROM merged_covid;
    """ 
    
        # PostgreSQL connection string
    
    # SQL query to retrieve data
    
    # Create a SQLAlchemy engine
    engine = create_engine(postgres_connection_string)
     
    # Execute the query and load the data into a DataFrame
    deaths_df = pd.read_sql_query(text(query), con=engine, parse_dates=['date_of_death'])
    # Close the database connection
    engine.dispose()
     
    # Bokeh plot
    output_file("deaths_over_time_chart.html")
    # Create a ColumnDataSource
    source = ColumnDataSource(deaths_df)
    # Create the figure
    p = figure(x_axis_label="Date", y_axis_label="Number of Deaths", title="Deaths Over Time",
               x_axis_type="datetime", width=800, height=400)
    

    # Line for new deaths
    p.line(x='date_of_death', y='new_deaths', line_width=2,line_color=Spectral11[0], source=source)
    # Line for cumulative deaths
    p.line(x='date_of_death', y='cumulative_deaths', line_width=2,line_color=Spectral11[1], source=source)
    # Add legend
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
     
    # Show the plot
    show(p)
    visualise2()
    
#Function to create Time Series plot for Cases Over Time  

def visualise2():
    postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/covid"
    query1 = """
    SELECT specimen_collection_date, new_cases, cumulative_cases FROM merged_covid;
    """
    engine = create_engine(postgres_connection_string)
    cases_df = pd.read_sql_query(text(query1), con=engine, parse_dates=['specimen_collection_date'])
    engine.dispose()
    output_file("cases_over_time_chart.html")
    source1 = ColumnDataSource(cases_df)
    q= figure(x_axis_label="Date", y_axis_label="Number of Cases", title="Cases Over Time",
           x_axis_type="datetime", width=800, height=400)    
    q.line(x='specimen_collection_date', y='new_cases', line_width=2, line_color=Spectral11[0], source=source1)
    q.line(x='specimen_collection_date', y='cumulative_cases', line_width=2,line_color=Spectral11[1], source=source1)
    show(q)
    visualise3()

#Function to create Time Series plot for Vaccination Over Time
    
def visualise3():    
    postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/covid"
     
    # SQL query to retrieve data
    query = """
        SELECT date_administered, cumulative_recipients, cumulative_2nd_booster_recipients FROM vaccinations_over_time;
    """
     
    # Create a SQLAlchemy engine
    engine = create_engine(postgres_connection_string)
     
    # Execute the query and load the data into a DataFrame
    cases_df = pd.read_sql_query(text(query), con=engine, parse_dates=['date_administered'])
     
    # Close the database connection
    engine.dispose()
     
    # Bokeh plot
    output_file("vaccination_over_time_chart.html")
     
    # Create a ColumnDataSource
    source = ColumnDataSource(cases_df)
     
    # Create the figure
    p = figure(x_axis_label="Date", y_axis_label="Number of Vaccinationated people", title="Vaccination Over Time",
               x_axis_type="datetime", width=800, height=400)
     
    # Line for new deaths
    line1 = p.line(x='date_administered', y='cumulative_recipients', line_width=2, line_color=Spectral11[1], source=source)
     
    # Line for cumulative deaths
    line2 = p.line(x='date_administered', y='cumulative_2nd_booster_recipients', line_width=2, line_color=Spectral11[3], source=source)
     
    # Add legend outside the graph
    #legend = Legend(items=[("Cumulative 1st dose recipients", [line1]), ("Cumulative 2nd dose recipients", [line2])], location="center", orientation="horizontal")
    #p.add_layout(legend, 'below')
     
    # Show the plot
    show(p)
    visualise4()

#Function to create Time Series plot for Testing Over Time

def visualise4():
    postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/covid"
    engine = create_engine(postgres_connection_string)
     
    # SQL query to retrieve Testing Over Time data

    query_testing = """
        SELECT
            specimen_collection_date,
            tests,
            pos,
            neg,
            indeterminate
        FROM
            merged_covid
    """
     
    # Execute the query and load the data into a DataFrame

    df_testing = pd.read_sql_query(text(query_testing), con=engine, parse_dates=['specimen_collection_date'])
     
    # Close the database connection

    engine.dispose()
     
    # Create a Bokeh figure

    p = figure(x_axis_label="Date", y_axis_label="Count", title="Testing Over Time",
               x_axis_type="datetime", width=800, height=400, toolbar_location="above")
     
    # Choose a color palette

    colors = Spectral11[:300]
     
    # Plot lines for each testing metric

    metrics = ['tests', 'pos', 'neg', 'indeterminate']
    for i, metric in enumerate(metrics):
        p.line(x='specimen_collection_date', y=metric, source=ColumnDataSource(df_testing),
                line_width=2, line_color=colors[i])
     
    # Display the legend

    p.legend.click_policy = "hide"
    p.legend.location = "top_left"
    # Show the Bokeh plot
    show(p)