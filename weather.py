#Downloading weather data using Python as a CSV using the Visual Crossing Weather API
#See https://www.visualcrossing.com/resources/blog/how-to-load-historical-weather-data-using-python-without-scraping/ for more information.
import csv
import codecs
import urllib.request
import urllib.error
import sys

# This is the core of our weather query URL
BaseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

ApiKey='sdsdsdsds'
#UnitGroup sets the units of the output - us or metric
UnitGroup='us'

#Location for the weather data
Location='Washington,DC'

#Optional start and end dates
#If nothing is specified, the forecast is retrieved. 
#If start date only is specified, a single historical or forecast day will be retrieved
#If both start and and end date are specified, a date range will be retrieved
StartDate = ''
EndDate=''

#JSON or CSV 
#JSON format supports daily, hourly, current conditions, weather alerts and events in a single JSON package
#CSV format requires an 'include' parameter below to indicate which table section is required
ContentType="csv"

#include sections
#values include days,hours,current,alerts
Include="days"


print('')
print(' - Requesting weather : ')

#basic query including location
ApiQuery=BaseURL + Location

#append the start and end date if present
if (len(StartDate)):
    ApiQuery+="/"+StartDate
    if (len(EndDate)):
        ApiQuery+="/"+EndDate

#Url is completed. Now add query parameters (could be passed as GET or POST)
ApiQuery+="?"

#append each parameter as necessary
if (len(UnitGroup)):
    ApiQuery+="&unitGroup="+UnitGroup

if (len(ContentType)):
    ApiQuery+="&contentType="+ContentType

if (len(Include)):
    ApiQuery+="&include="+Include

ApiQuery+="&key="+ApiKey



print(' - Running query URL: ', ApiQuery)
print()

try: 
    CSVBytes = urllib.request.urlopen(ApiQuery)
except urllib.error.HTTPError  as e:
    ErrorInfo= e.read().decode() 
    print('Error code: ', e.code, ErrorInfo)
    sys.exit()
except  urllib.error.URLError as e:
    ErrorInfo= e.read().decode() 
    print('Error code: ', e.code,ErrorInfo)
    sys.exit()


# Parse the results as CSV
CSVText = csv.reader(codecs.iterdecode(CSVBytes, 'utf-8'))
csv_file_path = 'weather_data.csv'
with open(csv_file_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerows(CSVText) 




import snowflake.connector

conn = snowflake.connector.connect(
    user='your_user',
    password='your_password',
    account='your_account',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema'


# Create table and define schema
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS weather_data (
CREATE TABLE IF NOT EXISTS weather_data(
name        VARCHAR,
datetime    VARCHAR,
tempmax VARCHAR,
tempmin VARCHAR,
temp VARCHAR,
feelslikemax VARCHAR,
feelslikemin VARCHAR,
feelslike VARCHAR,
dew VARCHAR,
humidity VARCHAR,
precip VARCHAR,
precipprob VARCHAR,
precipcover VARCHAR,
preciptype VARCHAR,
snow VARCHAR,
snowdepth VARCHAR,
windgust VARCHAR,
windspeed VARCHAR,
winddir VARCHAR,
sealevelpressure VARCHAR,
cloudcover VARCHAR,
visibility VARCHAR,
solarradiation VARCHAR,
solarenergy VARCHAR,
uvindex VARCHAR,
severerisk VARCHAR,
sunrise VARCHAR,
sunset VARCHAR,
moonphase VARCHAR,
conditions VARCHAR,
description VARCHAR,
icon VARCHAR,
stations VARCHAR
)  
)
""")

cur.execute("""
PUT file://{}/weather_data.csv @%weather_data
""".format(csv_file_path))

cur.execute("""
COPY INTO weather_data FROM @%weather_data FILE_FORMAT=(TYPE=csv FIELD_OPTIONALLY_ENCLOSED_BY='"')
""")



