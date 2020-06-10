import datetime as dt
import numpy as np
import pandas as pd
import json

#SQL Class containing corresponding queries
from sqlHelper import SQLHelper
from flask import Flask, jsonify

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

sqlHelper = SQLHelper()

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    return (
        f"Welcome to the Hawaii Data API!<br/>"

        f"""
        <ul>
            <li><a target="_blank" href='/api/v1.0/stations'>Get Station Details</a></li>
            <li><a target="_blank" href='/api/v1.0/precipitation'>Get All Precipitation</a></li>
            <li><a target="_blank" href='/api/v1.0/temperature'>Get Temperature for Most Active Station for Previous Year (Hard-Coded)</a></li>
            <li><a target="_blank" href='/api/v1.0/temperature/relative'>Get Temperature for Most Active Station for Previous Year (Relative Last 12 Months)</a></li>
            <li><a target="_blank" href='/api/v1.0/temperature/2016-08-24'>Get Temperature by Date</a></li>
            <li><a target="_blank" href='/api/v1.0/temperature/2016-08-24/2017-08-23'>Get Temperature by Date Range</a></li>
        </ul>
        """
    )


#return all stations
@app.route("/api/v1.0/stations")
def get_station_details(): 
    data = sqlHelper.getAllStationDetails()
    #jsonify returned dataframe
    return(jsonify(json.loads(data.to_json(orient='records')))) 


#return precipation aggregated by date
@app.route("/api/v1.0/precipitation")
def get_total_precipitation():
    data = sqlHelper.getTotalPrecipOnDate()
    #jsonify returned dataframe
    return(jsonify(json.loads(data.to_json(orient='records')))) 


#return previous year of temperature data for most active station
#HARD-CODED
@app.route("/api/v1.0/temperature")
def get_temp_most_active(): 
    data = sqlHelper.getTempForMostActive()
    #jsonify returned dataframe
    return(jsonify(json.loads(data.to_json(orient='records')))) 


#return previous year of temperature data for most active station
#RELATIVE 12 MONTHS
@app.route("/api/v1.0/temperature/relative")
def get_temp_most_active_relative(): 
    data = sqlHelper.getTempForMostActiveRelative()
    #jsonify returned dataframe
    return(jsonify(json.loads(data.to_json(orient='records')))) 


#return temperature by date (must be a string)
@app.route("/api/v1.0/temperature/<start>")
#accept a date as a string with format 2016-08-24
def get_temp_by_date(start): 
    data = sqlHelper.getTempByDate(start)
    #jsonify returned dataframe
    return(jsonify(json.loads(data.to_json(orient='records')))) 


#return temperature by date range (must be a string)
@app.route("/api/v1.0/temperature/<start>/<end>")
#accept a date as a string with format 2016-08-24
def get_temp_by_date_range(start, end): 
    data = sqlHelper.getTempByDateRange(start, end)
    #jsonify returned dataframe
    return(jsonify(json.loads(data.to_json(orient='records')))) 


#################################################
# Flask Run
#################################################
if __name__ == "__main__":
    app.run(debug=True)
