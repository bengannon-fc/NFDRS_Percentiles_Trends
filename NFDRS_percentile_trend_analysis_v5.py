# Created by: Matt Panunto (mpanunto@blm.gov) and Ben Gannon (benjamin.gannon@usda.gov)
# Created on: XX/XX/2023
# Last Updated: 10/21/2024

'''
Updates feature service with RAWS and PSA feature classes with observed and forecast fire weather/danger attributes from
WIMS with emphasis on ERC and BI percentiles and trends.
'''

# Import libraries and modules
import arcgis, os, sys, datetime, pandas, requests, statistics, urllib
import xml.etree.ElementTree as ET
from time import sleep
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from arcgis.geometry import filters
pandas.set_option('chained_assignment',None)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)

# Working directory (currently only for log file)
wdir = 'C:/Users/BenjaminGannon/Desktop/NFDRS_services'

# ArcGIS Online Portal URL
agol_portalurl = 'https://www.arcgis.com'

# Service item ID
erc_itemid = 'XXXXXXXX'

# ArcGIS Online Username
agol_username = 'XXXXXXXX'

# ArcGIS Online Password
agol_password = 'XXXXXXXX'

# Toggle for which day to use for WIMS data pull. Specify either 'Current Date' or specific date time string.
toggle_run_date = 'Current Date'
#toggle_run_date = '2024-07-22 14:00:00'

# Get datetime object for run day based on user inputs
if(toggle_run_date == 'Current Date'):
    datetime_today = datetime.datetime.today() #today
else:
    datetime_today = datetime.datetime.strptime(toggle_run_date, '%Y-%m-%d %H:%M:%S')

# Start log file and define function to print to both
lf = open(wdir + '/NFDRS_log_' + datetime_today.strftime('%m%d%Y') + '.txt', 'w')
def print_both(ptext):
    print(ptext)
    lf.write(ptext)

# Read in allstation and percentile tables
allstations = pandas.read_csv('C:/Users/BenjaminGannon/Desktop/NFDRS_services/Percentile_tables/AllStation.csv',
                              converters={'StationID': str})
percentiles = pandas.read_csv('C:/Users/BenjaminGannon/Desktop/NFDRS_services/Percentile_tables/Percentiles.csv',
                              converters={'StationID': str})

# Static attributes in RAWS layer to skip in updates
RAWS_static_attrs = ['OBJECTID','StationName','NESSID','NWSID','Elevation','Latitude','Longitude','State','County',
                     'Agency','Unit','StationID','MesoWestURL','Display','StnName_Clean','NWSID_Clean','GACC',
                     'Dispatch','PSA','FuelModelCode','GlobalID','CreationDate','Creator','EditDate','SHAPE']

# Create url variables of basic NFDRS and Observation urls to RAWS xml data
raws_nfdrs_url = 'https://famprod.nwcg.gov/prod-wims/xsql/nfdrs.xsql?stn=&sig=&type=N&fmodel=&start=&end=&time=&sort=&ndays=&user='
raws_nfdrs_fcast_url = 'https://famprod.nwcg.gov/prod-wims/xsql/nfdrs.xsql?stn=&sig=&type=F&fmodel=&start=&end=&time=&sort=&ndays=&user='
raws_obs_url = 'https://famprod.nwcg.gov/prod-wims/xsql/obs.xsql?stn=&sig=&type=&fmodel=&start=&end=&time=&sort=&ndays=&user='

# Create date variables to grab data for observation and forecast ranges
datetime_obs_start = datetime_today - datetime.timedelta(days=2)
datetime_tomorrow = datetime_today + datetime.timedelta(days=1)
datetime_for_end = datetime_today + datetime.timedelta(days=3)


#########################################################################################################################
### DATA SETUP
#########################################################################################################################
print_both('\r')
print_both('DATA SETUP\r')

# Create RAWS data frame for PSA level calcs
print_both('.CREATE RAWS 2 PSA TRANSFER TABLE\r')
raws2psa_df = allstations[['StationID','StationName']].drop_duplicates()
raws2psa_df = raws2psa_df.reset_index(drop=True)
raws2psa_df['ERC_per'] = pandas.NA
raws2psa_df['ERC_initial'] = pandas.NA
raws2psa_df['ERC_final'] = pandas.NA
raws2psa_df['ERC_fcast_per'] = pandas.NA
raws2psa_df['ERC_fcast_initial'] = pandas.NA
raws2psa_df['ERC_fcast_final'] = pandas.NA
raws2psa_df['BI_per'] = pandas.NA
raws2psa_df['BI_initial'] = pandas.NA
raws2psa_df['BI_final'] = pandas.NA
raws2psa_df['BI_fcast_per'] = pandas.NA
raws2psa_df['BI_fcast_initial'] = pandas.NA
raws2psa_df['BI_fcast_final'] = pandas.NA

# Establish connection to the ArcGIS Online Org
print_both('.REQUESTING API ACCESS TOKEN\r')
gis = GIS(agol_portalurl, agol_username, agol_password)

# Get RAWS/PSA Feature Service
print_both('.CONNECTING TO RAWS/PSA FEATURE SERVICE\r')
erc_service = gis.content.get(erc_itemid)
erc_layers = erc_service.layers

# Get RAWS layer
raws_layer = erc_layers[0]
raws_layer_url = raws_layer.url

# Get PSA layer
psa_layer = erc_layers[1]
psa_layer_url = psa_layer.url

# Query PSA feature service to subset to PSAs in the analysis
print_both('.SUBSET TO TARGET PSA DATA\r')
wherefield = 'PSANationalCode'
wherevalues = str(tuple(allstations['PSA'].tolist()))
whereClause = '"' + wherefield + '"' + ' IN ' + wherevalues
psa_query = psa_layer.query(where=whereClause)
psa_orig_sdf = psa_query.sdf
psa_update_sdf = psa_orig_sdf.sort_values(by=['PSANationalCode']) # Sort the dataframe by PSA Code

# Query RAWS feature service to subset to stations in the analysis
print_both('.SUBSET TO TARGET RAWS DATA\r')
wherefield = 'NWSID_clean'
wherevalues = str(tuple(list(str(n).zfill(6) for n in allstations['StationID'].tolist())))
whereClause = '"' + wherefield + '"' + ' IN ' + wherevalues
raws_query = raws_layer.query(where=whereClause)
raws_update_sdf = raws_query.sdf


#########################################################################################################################
### RAWS NFDRS PERCENTILES AND 3-DAY TRENDS
#########################################################################################################################
print_both('\r')
print_both('RAWS NFDRS PERCENTILES AND 3-DAY TRENDS\r')

for i in range(0, raws_update_sdf.shape[0]):
    
    print_both('.Processing ' + raws_update_sdf['NWSID_Clean'][i] + ', ' + raws_update_sdf['StnName_Clean'][i] + '\r')

    curr_NWSID = raws_update_sdf['NWSID_Clean'][i]

    try:
        
        # Grab the station's ERC and BI Percentile tables
        erc_per = percentiles.loc[(percentiles['StationID'] == curr_NWSID) & (percentiles['Component'] == 'ERC')]
        bi_per = percentiles.loc[(percentiles['StationID'] == curr_NWSID) & (percentiles['Component'] == 'BI')]

        ### GRAB DATA FROM WIMS
        
        # Build the NFDRS url for the current station
        curr_stationid_nfdrs_url = raws_nfdrs_url.replace('stn=', 'stn=' + curr_NWSID)
        curr_stationid_nfdrs_url = curr_stationid_nfdrs_url.replace('fmodel=', 'fmodel=' + str(raws_update_sdf['FuelModelCode'][i]))
        curr_stationid_nfdrs_url = curr_stationid_nfdrs_url.replace('start=', 'start=' + datetime_obs_start.strftime('%d-%b-%y'))
        curr_stationid_nfdrs_url = curr_stationid_nfdrs_url.replace('end=', 'end=' + datetime_today.strftime('%d-%b-%y'))

        # Build the NFDRS url with forecast information for the current station
        curr_stationid_nfdrs_fcast_url = raws_nfdrs_fcast_url.replace('stn=', 'stn=' + curr_NWSID)
        curr_stationid_nfdrs_fcast_url = curr_stationid_nfdrs_fcast_url.replace('fmodel=', 'fmodel=' + str(raws_update_sdf['FuelModelCode'][i]))
        curr_stationid_nfdrs_fcast_url = curr_stationid_nfdrs_fcast_url.replace('start=', 'start=' + datetime_today.strftime('%d-%b-%y'))
        curr_stationid_nfdrs_fcast_url = curr_stationid_nfdrs_fcast_url.replace('end=', 'end=' + datetime_for_end.strftime('%d-%b-%y'))

        # Build the Observation url for the current station
        curr_stationid_obs_url = raws_obs_url.replace('stn=', 'stn=' + curr_NWSID)
        curr_stationid_obs_url = curr_stationid_obs_url.replace('start=', 'start=' + datetime_today.strftime('%d-%b-%y'))
        curr_stationid_obs_url = curr_stationid_obs_url.replace('end=', 'end=' + datetime_today.strftime('%d-%b-%y'))    

        # Now convert the NFDRS xml data to a pandas dataframe
        print_both('..DOWNLOADING NFDRS DATA\r')
        nfdrs_xml_try = 0
        nfdrs_xml_download = False
        while(nfdrs_xml_download == False):
            try:
                # Try getting the 1300 data first
                xml_data = urllib.request.urlopen(curr_stationid_nfdrs_url.replace('time=','time=13'))
                root = ET.XML(xml_data.read())
                all_records = []
                for k, elem in enumerate(root):
                    record = {}
                    for child in elem:
                        record[child.tag] = child.text
                    all_records.append(record)
                curr_station_nfdrs_df = pandas.DataFrame(all_records)
                # If no results with the 1300 query, try getting the 1200 data
                if(len(curr_station_nfdrs_df) == 0):
                    xml_data = urllib.request.urlopen(curr_stationid_nfdrs_url.replace('time=','time=12'))
                    root = ET.XML(xml_data.read())
                    all_records = []
                    for k, elem in enumerate(root):
                        record = {}
                        for child in elem:
                            record[child.tag] = child.text
                        all_records.append(record)
                    curr_station_nfdrs_df = pandas.DataFrame(all_records)
                # If no results with the 1300 query or the 1200 query, try getting the 1400 data
                if(len(curr_station_nfdrs_df) == 0):
                    xml_data = urllib.request.urlopen(curr_stationid_nfdrs_url.replace('time=','time=14'))
                    root = ET.XML(xml_data.read())
                    all_records = []
                    for k, elem in enumerate(root):
                        record = {}
                        for child in elem:
                            record[child.tag] = child.text
                        all_records.append(record)
                    curr_station_nfdrs_df = pandas.DataFrame(all_records)
                nfdrs_xml_download = True
            except:
                if(nfdrs_xml_try < 5):
                    print_both('...NFDRS XML DOWNLOAD FAIL, RE-TRYING\r')
                    nfdrs_xml_try = nfdrs_xml_try + 1
                else:
                    print_both('...NFDRS XML DOWNLOAD FAIL 5 TIMES, SKIPPING STATION\r')
                    break

        # Now convert the NFDRS xml data to a pandas dataframe
        print_both('..DOWNLOADING NFDRS FORECAST DATA\r')
        nfdrs_xml_try = 0
        nfdrs_xml_download = False
        while(nfdrs_xml_download == False):
            try:
                xml_data = urllib.request.urlopen(curr_stationid_nfdrs_fcast_url)
                root = ET.XML(xml_data.read())
                all_records = []
                for k, elem in enumerate(root):
                    record = {}
                    for child in elem:
                        record[child.tag] = child.text
                    all_records.append(record)
                curr_station_nfdrs_fcast_df = pandas.DataFrame(all_records)
                nfdrs_xml_download = True
            except:
                if(nfdrs_xml_try < 5):
                    print_both('...NFDRS FORECAST XML DOWNLOAD FAIL, RE-TRYING\r')
                    nfdrs_xml_try = nfdrs_xml_try + 1
                else:
                    print_both('...NFDRS FORECAST XML DOWNLOAD FAIL 5 TIMES, SKIPPING STATION\r')
                    break

        # Now convert the Observation xml data to a pandas dataframe
        print_both('..DOWNLOADING OBS DATA\r')
        obs_xml_try = 0
        obs_xml_download = False
        while(obs_xml_download == False):
            try:
                xml_data = urllib.request.urlopen(curr_stationid_obs_url)
                root = ET.XML(xml_data.read())
                all_records = []
                for k, elem in enumerate(root):
                    record = {}
                    for child in elem:
                        record[child.tag] = child.text
                    all_records.append(record)
                curr_station_obs_df = pandas.DataFrame(all_records)
                obs_xml_download = True
            except:
                if(obs_xml_try < 5):
                    print_both('...OBS XML DOWNLOAD FAIL, RE-TRYING\r')
                    obs_xml_try = obs_xml_try + 1
                else:
                    print_both('...OBS XML DOWNLOAD FAIL 5 TIMES, SKIPPING STATION\r')
                    break

        # Sort NFDR Observation dataframe by 'nfdr_dt', then by 'mp'
        # Then create a 'nfdr_dt_tm' field
        # Then parse the 'nfdr_dt_tm' field to an actual datetime field, and sort by this field.
        # Also sort by 'mp' (model priority). See further below for why this is important.
        nfdr_dt_list = list(curr_station_nfdrs_df['nfdr_dt'])
        nfdr_tm_list = list(curr_station_nfdrs_df['nfdr_tm'])
        nfdr_dt_tm_list = [k + ' ' + l for k, l in zip(nfdr_dt_list, nfdr_tm_list)]
        curr_station_nfdrs_df['nfdr_dt_tm'] = nfdr_dt_tm_list
        curr_station_nfdrs_df['nfdr_datetime'] = pandas.to_datetime(curr_station_nfdrs_df['nfdr_dt_tm'], format='%m/%d/%Y %H')
        curr_station_nfdrs_df = curr_station_nfdrs_df[(curr_station_nfdrs_df['nfdr_dt'] == datetime_obs_start.strftime('%m/%d/%Y')) | (curr_station_nfdrs_df['nfdr_dt'] == datetime_today.strftime('%m/%d/%Y'))]
        curr_station_nfdrs_df = curr_station_nfdrs_df.sort_values(by=['nfdr_datetime', 'mp'], ascending = [True, True])
        curr_station_nfdrs_df.reset_index(drop=True)

        # Sort NFDR Forecast dataframe by 'nfdr_dt', then by 'mp'
        # Then create a 'nfdr_dt_tm' field
        # Then parse the 'nfdr_dt_tm' field to an actual datetime field, and sort by this field.
        # Also sort by 'mp' (model priority). See further below for why this is important.
        # Lastly, keep only forecasted observations, and only for the next 3 days ########### Moved to data call
        if(curr_station_nfdrs_fcast_df.shape[0] > 0):
            nfdr_dt_list = list(curr_station_nfdrs_fcast_df['nfdr_dt'])
            nfdr_tm_list = list(curr_station_nfdrs_fcast_df['nfdr_tm'])
            nfdr_dt_tm_list = [k + ' ' + l for k, l in zip(nfdr_dt_list, nfdr_tm_list)]
            curr_station_nfdrs_fcast_df['nfdr_dt_tm'] = nfdr_dt_tm_list
            curr_station_nfdrs_fcast_df['nfdr_datetime'] = pandas.to_datetime(curr_station_nfdrs_fcast_df['nfdr_dt_tm'], format='%m/%d/%Y %H')
            curr_station_nfdrs_fcast_df = curr_station_nfdrs_fcast_df[(curr_station_nfdrs_fcast_df['nfdr_dt'] == datetime_tomorrow.strftime('%m/%d/%Y')) | (curr_station_nfdrs_fcast_df['nfdr_dt'] == datetime_for_end.strftime('%m/%d/%Y'))] 
            curr_station_nfdrs_fcast_df = curr_station_nfdrs_fcast_df.sort_values(by=['nfdr_datetime', 'mp'], ascending = [True, True])
            curr_station_nfdrs_fcast_df.reset_index(drop=True)

        # Sort Observation dataframe by 'obs_dt' and 'obs_tm'
        # Then create a 'obs_dt_tm' field
        # Then parse the 'obs_dt_tm' field to an actual datetime field, and sort by this field.
        obs_dt_list = list(curr_station_obs_df['obs_dt'])
        obs_tm_list = list(curr_station_obs_df['obs_tm'])
        obs_dt_tm_list = [k + ' ' + l for k, l in zip(obs_dt_list, obs_tm_list)]
        curr_station_obs_df['obs_dt_tm'] = obs_dt_tm_list
        curr_station_obs_df['obs_datetime'] = pandas.to_datetime(curr_station_obs_df['obs_dt_tm'], format='%m/%d/%Y %H')
        curr_station_obs_df = curr_station_obs_df.sort_values(by=['obs_datetime'], ascending = [True])

        # Subset to observation at assessment day's reporting time
        nfdr_dt_aday = curr_station_nfdrs_df[curr_station_nfdrs_df['nfdr_dt'] == datetime_today.strftime('%m/%d/%Y')]['nfdr_datetime'][0]
        curr_station_obs_df_filtered = curr_station_obs_df[curr_station_obs_df['obs_datetime'] == nfdr_dt_aday]

        # Merge the NFDRS and Obs dataframes together
        if( len(curr_station_obs_df_filtered) == 1 ):
            curr_station_nfdrs_obs_df = pandas.merge(curr_station_nfdrs_df,curr_station_obs_df_filtered,how='left',
                                                     left_on='nfdr_dt_tm',right_on='obs_dt_tm',suffixes=('', '_y'))
        else:
            curr_station_nfdrs_obs_df = curr_station_nfdrs_df

        # Want only a single result for each day, and want to keep the record with the lowest 'mp' value (model priority).
        # This is done by removing results that are duplicates based on date, and keeping only the first occurrence of the date.
        # If there is a duplicate, it exists because the 'mp' value is different
        curr_station_nfdrs_obs_df = curr_station_nfdrs_obs_df.drop_duplicates(subset='nfdr_dt',keep='first')
        curr_station_nfdrs_fcast_df = curr_station_nfdrs_fcast_df.drop_duplicates(subset='nfdr_dt',keep='first')


        #################################################################################################################
        ### DETERMINE RAWS ERC PERCENTILE AND 3-DAY TRENDS
        #################################################################################################################

        print_both('..PROCESSING ERC\r')

        # Create list of ERC values
        curr_stationid_erc_list = list(curr_station_nfdrs_obs_df['ec'])
        
        ### Determine ERC Percentile if current day's data is available
        latest_obs_datetime = max(curr_station_nfdrs_obs_df['nfdr_datetime'])
        latest_obs_date_str = latest_obs_datetime.strftime('%Y%m%d')
        
        if(latest_obs_date_str == datetime_today.strftime('%Y%m%d')):

            # Determine ERC Percentile
            print_both('...DETERMINING ERC PERCENTILE\r')
            latest_erc = float(list(curr_station_nfdrs_obs_df['ec'])[len(curr_station_nfdrs_obs_df['ec'])-1])
            for k in range(0, len(erc_per)):

                curr_erc_range_lowerbound = list(erc_per['GreaterThanEqualTo'])[k]
                curr_erc_range_upperbound = list(erc_per['LessThan'])[k]
                curr_erc_range_percentile = list(erc_per['Percentile'])[k]
                if(curr_erc_range_lowerbound <= latest_erc < curr_erc_range_upperbound):
                    curr_stationid_erc_percentile = curr_erc_range_percentile
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_per'] = curr_stationid_erc_percentile
                    break

                # If its the first iteration, and the erc value is lower than the lowerbound value, set erc percentile to 0
                if(k == 0 and latest_erc < curr_erc_range_lowerbound):
                    curr_stationid_erc_percentile = 0
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_per'] = curr_stationid_erc_percentile
                    break

                # If its the last iteration, and the erc value is higher than the upperbound value, set erc percentile to 100
                if( (k == (len(erc_per) - 1)) and latest_erc > curr_erc_range_upperbound):
                    curr_stationid_erc_percentile = 100
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_per'] = curr_stationid_erc_percentile
                    break

                # If its the last iteration, and no percentile has been found, warn user
                if(k == (len(erc_per) - 1) ):
                    print_both('....UNABLE TO DETERMINE ERC PERCENTILE FOR STATION ID: ' + curr_NWSID + ' (' + raws_update_sdf['StnName_Clean'][i] + ')\r')


            # If have the last 3 days of data, determine trend
            print_both('...DETERMINING ERC TREND (LAST 3-DAYS)\r')
            if(len(curr_stationid_erc_list) < 2):
                print_both('....DOES NOT HAVE 3 DAYS WORTH OF DATA, UNABLE TO DETERMINE TREND\r')
                curr_stationid_erc_trend = pandas.NA
            else:
                # Get initial and final ERC value
                curr_station_erc_initial = float(curr_stationid_erc_list[0])
                curr_station_erc_final = float(curr_stationid_erc_list[1])

                # Determine ERC trend
                curr_station_erc_diff = curr_station_erc_final - curr_station_erc_initial
                curr_station_erc_diff_abs = abs(curr_station_erc_diff)

                # Increasing
                if((curr_station_erc_final - curr_station_erc_initial) >= 3):
                    curr_stationid_erc_trend = 'Increase'
                    print_both('....TRENDING: ' + curr_stationid_erc_trend + ' (UP ' + str(round(curr_station_erc_diff_abs, 1)) + ')\r')

                # Decreasing
                if((curr_station_erc_final - curr_station_erc_initial) <= -3):
                    curr_stationid_erc_trend = 'Decrease'
                    print_both('....TRENDING: ' + curr_stationid_erc_trend + ' (DOWN ' + str(round(curr_station_erc_diff_abs, 1)) + ')\r')

                # No Change
                if(curr_station_erc_diff_abs < 3):
                    curr_stationid_erc_trend = 'No Change'
                    if(curr_station_erc_diff > 0):
                        print_both('....TRENDING: ' + curr_stationid_erc_trend + ' (UP ' + str(round(curr_station_erc_diff_abs, 1)) + ')\r')
                    if(curr_station_erc_diff < 0):
                        print_both('....TRENDING: ' + curr_stationid_erc_trend + ' (DOWN ' + str(round(curr_station_erc_diff_abs, 1)) + ')\r')
                    if(curr_station_erc_diff == 0):
                        print_both('....TRENDING: ' + curr_stationid_erc_trend + ' (' + str(round(curr_station_erc_diff_abs, 1)) + ')\r')

                # Save to data frame for calculating PSA average
                raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_initial'] = curr_station_erc_initial
                raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_final'] = curr_station_erc_final

        else:
            # Failed the current date test
            print_both('...NO NEW ERC DATA AVAILABLE FOR TODAY\r')

        
        ### Determine 1-Day Forecast ERC Percentile, if tomorrow's forecasted data is available
        if(curr_station_nfdrs_fcast_df.shape[0] > 0):
            fcast_obs_datetime = min(curr_station_nfdrs_fcast_df['nfdr_datetime'])
            fcast_obs_date_str = fcast_obs_datetime.strftime('%Y%m%d')
        else:
            fcast_erc = pandas.NA
            curr_stationid_erc_1day_fcast_percentile = pandas.NA
            fcast_obs_date_str = 'No Data'
        if(fcast_obs_date_str == datetime_tomorrow.strftime('%Y%m%d')):

            # Determine ERC Percentile
            print_both('...DETERMINING ERC PERCENTILE (NEXT 1-DAY FORECAST)\r')
            fcast_erc = float(list(curr_station_nfdrs_fcast_df['ec'])[0])
            for k in range(0, len(erc_per)):

                curr_erc_range_lowerbound = list(erc_per['GreaterThanEqualTo'])[k]
                curr_erc_range_upperbound = list(erc_per['LessThan'])[k]
                curr_erc_range_percentile = list(erc_per['Percentile'])[k]
                if(curr_erc_range_lowerbound <= fcast_erc < curr_erc_range_upperbound):
                    curr_stationid_erc_1day_fcast_percentile = curr_erc_range_percentile
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_fcast_per'] = curr_stationid_erc_1day_fcast_percentile
                    break

                # If its the first iteration, and the erc value is lower than the lowerbound value, set erc percentile to 0
                if(k == 0 and fcast_erc < curr_erc_range_lowerbound):
                    curr_stationid_erc_1day_fcast_percentile = 0
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_fcast_per'] = curr_stationid_erc_1day_fcast_percentile
                    break

                # If its the last iteration, and the erc value is higher than the upperbound value, set erc percentile to 100
                if( (k == (len(erc_per) - 1)) and fcast_erc > curr_erc_range_upperbound):
                    curr_stationid_erc_1day_fcast_percentile = 100
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_fcast_per'] = curr_stationid_erc_1day_fcast_percentile
                    break

                # If its the last iteration, and no percentile has been found, warn user
                if(k == (len(erc_per) - 1) ):
                    print_both('....UNABLE TO DETERMINE ERC PERCENTILE FOR STATION ID: ' + curr_NWSID + ' (' + raws_update_sdf['StnName_Clean'][i] + ')\r')


        # Determine 3-day Forecast ERC Trend, if the next 3 days of forecasted data is available
        print_both('...DETERMINING ERC TREND (NEXT 3-DAY FORECAST)\r')
        if(curr_station_nfdrs_fcast_df.shape[0] < 2):
            print_both('....DOES NOT HAVE 3 DAYS WORTH OF DATA, UNABLE TO DETERMINE FORECAST TREND\r')
            curr_stationid_erc_fcast_trend = pandas.NA
        else:
            # Create list of forecasted ERC values
            curr_stationid_erc_fcast_list = list(curr_station_nfdrs_fcast_df['ec'])
        
            # Get initial and final ERC forecast value
            curr_station_erc_fcast_initial = float(curr_stationid_erc_fcast_list[0])
            curr_station_erc_fcast_final = float(curr_stationid_erc_fcast_list[1])

            # Determine ERC forecast trend
            curr_station_erc_fcast_diff = curr_station_erc_fcast_final - curr_station_erc_fcast_initial
            curr_station_erc_fcast_diff_abs = abs(curr_station_erc_fcast_diff)

            # Increasing
            if((curr_station_erc_fcast_final - curr_station_erc_fcast_initial) >= 3):
                curr_stationid_erc_fcast_trend = 'Increase'
                print_both('....TRENDING: ' + curr_stationid_erc_fcast_trend + ' (UP ' + str(round(curr_station_erc_fcast_diff_abs, 1)) + ')\r')

            # Decreasing
            if((curr_station_erc_fcast_final - curr_station_erc_fcast_initial) <= -3):
                curr_stationid_erc_fcast_trend = 'Decrease'
                print_both('....TRENDING: ' + curr_stationid_erc_fcast_trend + ' (DOWN ' + str(round(curr_station_erc_fcast_diff_abs, 1)) + ')\r')

            # No Change
            if(curr_station_erc_fcast_diff_abs < 3):
                curr_stationid_erc_fcast_trend = 'No Change'
                if(curr_station_erc_fcast_diff > 0):
                    print_both('....TRENDING: ' + curr_stationid_erc_fcast_trend + ' (UP ' + str(round(curr_station_erc_fcast_diff_abs, 1)) + ')\r')
                if(curr_station_erc_fcast_diff < 0):
                    print_both('....TRENDING: ' + curr_stationid_erc_fcast_trend + ' (DOWN ' + str(round(curr_station_erc_fcast_diff_abs, 1)) + ')\r')
                if(curr_station_erc_fcast_diff == 0):
                    print_both('....TRENDING: ' + curr_stationid_erc_fcast_trend + ' (' + str(round(curr_station_erc_fcast_diff_abs, 1)) + ')\r')

            # Save to data frame for calculating PSA average
            raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_fcast_initial'] = curr_station_erc_fcast_initial
            raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'ERC_fcast_final'] = curr_station_erc_fcast_final

        
        #################################################################################################################
        ### DETERMINE RAWS BI PERCENTILE AND 3-DAY TREND
        #################################################################################################################

        print_both('..PROCESSING BI\r')

        # Create list of BI values
        curr_stationid_bi_list = list(curr_station_nfdrs_obs_df['bi'])

        # Determine BI Percentile if current day's data is available
        latest_obs_datetime = max(curr_station_nfdrs_obs_df['nfdr_datetime'])
        latest_obs_date_str = latest_obs_datetime.strftime('%Y%m%d')
        if(latest_obs_date_str == datetime_today.strftime('%Y%m%d')):

            # Determine BI Percentile
            print_both('...DETERMINING BI PERCENTILE\r')
            latest_bi = float(list(curr_station_nfdrs_obs_df['bi'])[len(curr_station_nfdrs_obs_df['bi'])-1])
            for k in range(0, len(bi_per)):

                curr_bi_range_lowerbound = list(bi_per['GreaterThanEqualTo'])[k]
                curr_bi_range_upperbound = list(bi_per['LessThan'])[k]
                curr_bi_range_percentile = list(bi_per['Percentile'])[k]
                if(curr_bi_range_lowerbound <= latest_bi < curr_bi_range_upperbound):
                    curr_stationid_bi_percentile = curr_bi_range_percentile
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_per'] = curr_stationid_bi_percentile
                    break

                # If its the first iteration, and the bi value is lower than the lowerbound value, set bi percentile to 0
                if(k == 0 and latest_bi < curr_bi_range_lowerbound):
                    curr_stationid_bi_percentile = 0
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_per'] = curr_stationid_bi_percentile
                    break

                # If its the last iteration, and the bi value is higher than the upperbound value, set bi percentile to 100
                if( (k == (len(bi_per) - 1)) and latest_bi > curr_bi_range_upperbound):
                    curr_stationid_bi_percentile = 100
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_per'] = curr_stationid_bi_percentile
                    break

                # If its the last iteration, and no percentile has been found, warn user
                if(k == (len(bi_per) - 1) ):
                    print_both('....UNABLE TO DETERMINE BI PERCENTILE FOR STATION ID: ' + curr_NWSID + ' (' + raws_update_sdf['StnName_Clean'][i] + ')\r')


            # If have the last 3 days of data, determine trend
            print_both('...DETERMINING BI TREND (LAST 3-DAYS)\r')
            if(len(curr_stationid_bi_list) < 2):
                print_both('....DOES NOT HAVE 3 DAYS WORTH OF DATA, UNABLE TO DETERMINE TREND\r')
                curr_stationid_bi_trend = pandas.NA
            else:
                # Get initial and final BI value
                curr_station_bi_initial = float(curr_stationid_bi_list[0])
                curr_station_bi_final = float(curr_stationid_bi_list[1])

                # Determine BI trend
                curr_station_bi_diff = curr_station_bi_final - curr_station_bi_initial
                curr_station_bi_diff_abs = abs(curr_station_bi_diff)

                # Increasing
                if((curr_station_bi_final - curr_station_bi_initial) >= 3):
                    curr_stationid_bi_trend = 'Increase'
                    print_both('....TRENDING: ' + curr_stationid_bi_trend + ' (UP ' + str(round(curr_station_bi_diff_abs, 1)) + ')\r')

                # Decreasing
                if((curr_station_bi_final - curr_station_bi_initial) <= -3):
                    curr_stationid_bi_trend = 'Decrease'
                    print_both('....TRENDING: ' + curr_stationid_bi_trend + ' (DOWN ' + str(round(curr_station_bi_diff_abs, 1)) + ')\r')

                # No Change
                if(curr_station_bi_diff_abs < 3):
                    curr_stationid_bi_trend = 'No Change'
                    if(curr_station_bi_diff > 0):
                        print_both('....TRENDING: ' + curr_stationid_bi_trend + ' (UP ' + str(round(curr_station_bi_diff_abs, 1)) + ')\r')
                    if(curr_station_bi_diff < 0):
                        print_both('....TRENDING: ' + curr_stationid_bi_trend + ' (DOWN ' + str(round(curr_station_bi_diff_abs, 1)) + ')\r')
                    if(curr_station_bi_diff == 0):
                        print_both('....TRENDING: ' + curr_stationid_bi_trend + ' (' + str(round(curr_station_bi_diff_abs, 1)) + ')\r')

                # Save to data frame for calculating PSA average
                raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_initial'] = curr_station_bi_initial
                raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_final'] = curr_station_bi_final

        else:
            # Failed the current date test
            print_both('...NO NEW BI DATA AVAILABLE FOR TODAY\r')

        
        ### Determine 1-Day Forecast BI Percentile, if tomorrow's forecasted data is available
        if(curr_station_nfdrs_fcast_df.shape[0] > 0):
            fcast_obs_datetime = min(curr_station_nfdrs_fcast_df['nfdr_datetime'])
            fcast_obs_date_str = fcast_obs_datetime.strftime('%Y%m%d')
        else:
            fcast_bi = pandas.NA
            curr_stationid_bi_1day_fcast_percentile = pandas.NA
            fcast_obs_date_str = 'No Data'
        if(fcast_obs_date_str == datetime_tomorrow.strftime('%Y%m%d')):

            # Determine BI Percentile
            print_both('...DETERMINING BI PERCENTILE (NEXT 1-DAY FORECAST)\r')
            fcast_bi = float(list(curr_station_nfdrs_fcast_df['bi'])[0])
            for k in range(0, len(bi_per)):

                curr_bi_range_lowerbound = list(bi_per['GreaterThanEqualTo'])[k]
                curr_bi_range_upperbound = list(bi_per['LessThan'])[k]
                curr_bi_range_percentile = list(bi_per['Percentile'])[k]
                if(curr_bi_range_lowerbound <= fcast_bi < curr_bi_range_upperbound):
                    curr_stationid_bi_1day_fcast_percentile = curr_bi_range_percentile
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_fcast_per'] = curr_stationid_bi_1day_fcast_percentile
                    break

                # If its the first iteration, and the bi value is lower than the lowerbound value, set bi percentile to 0
                if(k == 0 and fcast_bi < curr_bi_range_lowerbound):
                    curr_stationid_bi_1day_fcast_percentile = 0
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_fcast_per'] = curr_stationid_bi_1day_fcast_percentile
                    break

                # If its the last iteration, and the bi value is higher than the upperbound value, set bi percentile to 100
                if( (k == (len(bi_per) - 1)) and fcast_bi > curr_bi_range_upperbound):
                    curr_stationid_bi_1day_fcast_percentile = 100
                    raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_fcast_per'] = curr_stationid_bi_1day_fcast_percentile
                    break

                # If its the last iteration, and no percentile has been found, warn user
                if(k == (len(bi_per) - 1) ):
                    print_both('....UNABLE TO DETERMINE BI PERCENTILE FOR STATION ID: ' + curr_NWSID + ' (' + raws_update_sdf['StnName_Clean'][i] + ')\r')

        # Determine 3-day Forecast BI Trend, if the next 3 days of forecasted data is available
        print_both('...DETERMINING BI TREND (NEXT 3-DAY FORECAST)\r')
        if(curr_station_nfdrs_fcast_df.shape[0] < 2):
            print_both('....DOES NOT HAVE 3 DAYS WORTH OF DATA, UNABLE TO DETERMINE FORECAST TREND\r')
            curr_stationid_bi_fcast_trend = pandas.NA
        else:
            # Create list of forecasted BI values
            curr_stationid_bi_fcast_list = list(curr_station_nfdrs_fcast_df['bi'])
        
            # Get initial and final BI forecast value
            curr_station_bi_fcast_initial = float(curr_stationid_bi_fcast_list[0])
            curr_station_bi_fcast_final = float(curr_stationid_bi_fcast_list[1])

            # Determine BI forecast trend
            curr_station_bi_fcast_diff = curr_station_bi_fcast_final - curr_station_bi_fcast_initial
            curr_station_bi_fcast_diff_abs = abs(curr_station_bi_fcast_diff)

            # Increasing
            if((curr_station_bi_fcast_final - curr_station_bi_fcast_initial) >= 3):
                curr_stationid_bi_fcast_trend = 'Increase'
                print_both('....TRENDING: ' + curr_stationid_bi_fcast_trend + ' (UP ' + str(round(curr_station_bi_fcast_diff_abs, 1)) + ')\r')

            # Decreasing
            if((curr_station_bi_fcast_final - curr_station_bi_fcast_initial) <= -3):
                curr_stationid_bi_fcast_trend = 'Decrease'
                print_both('....TRENDING: ' + curr_stationid_bi_fcast_trend + ' (DOWN ' + str(round(curr_station_bi_fcast_diff_abs, 1)) + ')\r')

            # No Change
            if(curr_station_bi_fcast_diff_abs < 3):
                curr_stationid_bi_fcast_trend = 'No Change'
                if(curr_station_bi_fcast_diff > 0):
                    print_both('....TRENDING: ' + curr_stationid_bi_fcast_trend + ' (UP ' + str(round(curr_station_bi_fcast_diff_abs, 1)) + ')\r')
                if(curr_station_bi_fcast_diff < 0):
                    print_both('....TRENDING: ' + curr_stationid_bi_fcast_trend + ' (DOWN ' + str(round(curr_station_bi_fcast_diff_abs, 1)) + ')\r')
                if(curr_station_bi_fcast_diff == 0):
                    print_both('....TRENDING: ' + curr_stationid_bi_fcast_trend + ' (' + str(round(curr_station_bi_fcast_diff_abs, 1)) + ')\r')

            # Save to data frame for calculating PSA average
            raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_fcast_initial'] = curr_station_bi_fcast_initial
            raws2psa_df.loc[(raws2psa_df['StationID'] == curr_NWSID), 'BI_fcast_final'] = curr_station_bi_fcast_final
        

        #############################################################################################
        ### INSERT VALUES INTO RAWS UPDATE DATAFRAME
        #############################################################################################

        # Create dataframe of latest observation values from WIMS
        print_both('..INSERTING VALUES INTO UPDATE DATAFRAME\r')
        max_datetime = max(curr_station_nfdrs_obs_df['nfdr_datetime'])
        latest_stationid_df = curr_station_nfdrs_obs_df[curr_station_nfdrs_obs_df['nfdr_datetime'] == max_datetime]
        wims_columns = list(latest_stationid_df.columns)

        # Now loop through all columns of the raws_update_sdf, and insert the new values for the current station
        raws_update_columns = list(raws_update_sdf.columns)
        for k in range(0, len(raws_update_columns)):
            try:

                # Get current column, and it's value
                curr_column = raws_update_columns[k]

                # Skip any columns that aren't needed
                if(curr_column in RAWS_static_attrs):
                    continue

                # Enter NA into the 'raws_update_sdf' if there was a problem with the merge, or if there aren't any observations from WIMS for today
                if( latest_obs_date_str != datetime_today.strftime('%Y%m%d') ):
                    raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = pandas.NA
                    continue
                else:
                    # If no issue with merge or observation date, insert actual value
                    # If current column is any of the following fields that are not in WIMS, need to insert specific variable
                    if(curr_column == 'NFDRS_Data_URL'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_nfdrs_url
                        continue
                    if(curr_column == 'Obs_Data_URL'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_obs_url
                        continue
                    if(curr_column == 'ec_percentile'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_erc_percentile
                        continue
                    if(curr_column == 'ec_trend'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_erc_trend
                        continue
                    if(curr_column == 'ec_fcast'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = fcast_erc
                        continue
                    if(curr_column == 'ec_fcast_percentile'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_erc_1day_fcast_percentile
                        continue
                    if(curr_column == 'ec_fcast_trend'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_erc_fcast_trend
                        continue
                    if(curr_column == 'bi_percentile'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_bi_percentile
                        continue
                    if(curr_column == 'bi_trend'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_bi_trend
                        continue
                    if(curr_column == 'bi_fcast'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = fcast_bi
                        continue
                    if(curr_column == 'bi_fcast_percentile'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_bi_1day_fcast_percentile
                        continue
                    if(curr_column == 'bi_fcast_trend'):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_bi_fcast_trend
                        continue

                    # If current column is not in WIMS, but is also not any of the fields above, Enter NA into the 'raws_update_sdf'
                    if(curr_column not in wims_columns):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = pandas.NA
                        continue

                    # Otherwise, get the current value in WIMS for the column, and also get the type
                    curr_value = latest_stationid_df[curr_column].iloc[0]
                    curr_value_str = str(curr_value)
                    curr_value_type = str(type(curr_value))

                    # If value is NA, insert NA into 'raws_update_sdf'
                    if(curr_value_str in ['<NA>', 'nan']):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = pandas.NA
                        continue

                    # If value is a pandas Timestamp type, insert as-is into 'raws_update_sdf'
                    if('Timestamp' in curr_value_type):
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_value
                        continue

                    # If value is an integer, set to int before inserting into 'raws_update_sdf'
                    if(curr_value_str.isnumeric()):
                        curr_value = int(curr_value)
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_value
                        continue

                    # If value has a period, that means it is a float, set to float before inserting into 'raws_update_sdf'
                    if('.' in curr_value_str):
                        curr_value = float(curr_value)
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_value
                        continue
                    
                    # If field is staffing level 'sl' only keep integer portion (first character)
                    if(curr_column == 'sl'):
                        curr_value = int(curr_value[0])
                        raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_value
                        continue
                        
                    # Otherwise, insert value as-is into 'raws_update_sdf'
                    raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_value


            except Exception as e:

                print_both('...COLUMN:' + curr_column + '\r')
                print_both('...UNABLE TO INSERT VALUE, INSERTING NA\r')
                print_both('...' + str(e) + '\r')
                break
                raws_update_sdf.loc[(raws_update_sdf['NWSID'] == curr_NWSID), curr_column] = pandas.NA
                
    except Exception as e:

        print_both('..ERROR:\r')
        print_both(str(e))
        print_both('\r')
        print_both('..INSERTING NULL VALUES INTO FEATURE SERVICE\r')

        # Now loop through all columns of the raws_update_sdf, and insert the values in for the current station
        raws_update_columns = list(raws_update_sdf.columns)
        for k in range(0, len(raws_update_columns)):

                # Get current column, and it's value
                curr_column = raws_update_columns[k]

                # Skip any columns that aren't needed
                if(curr_column in RAWS_static_attrs):
                    continue

                # Enter the WIMS urls into the dataframe
                if(curr_column == 'NFDRS_Data_URL'):
                    raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_nfdrs_url
                    continue
                if(curr_column == 'Obs_Data_URL'):
                    raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = curr_stationid_obs_url
                    continue

                # Insert NA for all other columns
                raws_update_sdf.loc[(raws_update_sdf['NWSID_Clean'] == curr_NWSID), curr_column] = pandas.NA

print_both('\r')


#####################################################################################################
### PSA NFDRS PERCENTILES AND 3-DAY TRENDS
#####################################################################################################
print_both('\r')
print_both('PSA NFDRS PERCENTILES AND 3-DAY TRENDS\r')

# Get list of PSAs to update
PSAs = sorted(list(set(allstations['PSA'].tolist())))
PSAs = [PSA for PSA in PSAs if PSA != 'Non-PSA'] # Ignore non-PSA stations

for i in range(0, len(PSAs)):

    print_both('.Processing PSA ' + PSAs[i] + '\r')
    
    try:

        # Get list of RAWS in PSA
        RAWS_list = allstations.loc[allstations['PSA'] == PSAs[i],'StationID'].tolist()
        
        # Loop through each station in the PSA
        curr_psa_erc_initial_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'ERC_initial'].dropna().tolist()
        curr_psa_erc_final_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'ERC_final'].dropna().tolist()
        curr_psa_erc_percentile_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'ERC_per'].dropna().tolist()
        curr_psa_erc_1day_fcast_percentile_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'ERC_fcast_per'].dropna().tolist()
        curr_psa_erc_3day_fcast_initial_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'ERC_fcast_initial'].dropna().tolist()
        curr_psa_erc_3day_fcast_final_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'ERC_fcast_final'].dropna().tolist()
        curr_psa_bi_initial_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'BI_initial'].dropna().tolist()
        curr_psa_bi_final_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'BI_final'].dropna().tolist()
        curr_psa_bi_percentile_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'BI_per'].dropna().tolist()
        curr_psa_bi_1day_fcast_percentile_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'BI_fcast_per'].dropna().tolist()
        curr_psa_bi_3day_fcast_initial_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'BI_fcast_initial'].dropna().tolist()
        curr_psa_bi_3day_fcast_final_list = raws2psa_df.loc[raws2psa_df['StationID'].isin(RAWS_list),'BI_fcast_final'].dropna().tolist()
        
        ### ERC ####

        # Calculate the PSA average ERC Percentile
        if(len(curr_psa_erc_percentile_list) > 0):
            curr_psa_avg_erc_percentile = statistics.mean(curr_psa_erc_percentile_list)
            curr_psa_avg_erc_percentile_round = round(curr_psa_avg_erc_percentile, 2)
            print_both('..PSA ERC PER MEAN: ' + str(curr_psa_avg_erc_percentile_round) + '\r')
        else:
            curr_psa_avg_erc_percentile_round = pandas.NA
            print_both('..PSA ERC PER MEAN: NO OBSERVATIONS\r')
            
        # Calculate the PSA average ERC Forecast Percentile
        if(len(curr_psa_erc_1day_fcast_percentile_list) > 0):
            curr_psa_avg_erc_1day_fcast_percentile = statistics.mean(curr_psa_erc_1day_fcast_percentile_list)
            curr_psa_avg_erc_1day_fcast_percentile_round = round(curr_psa_avg_erc_1day_fcast_percentile, 2)
            print_both('..PSA ERC PER FORECAST MEAN: ' + str(curr_psa_avg_erc_1day_fcast_percentile_round) + '\r')
        else:
            curr_psa_avg_erc_1day_fcast_percentile_round = pandas.NA
            print_both('..PSA ERC PER FORECAST MEAN: NO OBSERVATIONS\r')
        
        # Determine PSA ERC trend
        if( (len(curr_psa_erc_initial_list) > 0) & (len(curr_psa_erc_final_list) > 0) ):
            # First, get PSA initial and final average ERC (average of 1st day, and average of 3rd day)
            curr_psa_erc_initial_avg = statistics.mean(curr_psa_erc_initial_list)
            curr_psa_erc_final_avg = statistics.mean(curr_psa_erc_final_list)

            # Now determine the difference between the average 1st day, and average 3rd day
            curr_psa_erc_diff = curr_psa_erc_final_avg - curr_psa_erc_initial_avg
            curr_psa_erc_diff_abs = abs(curr_psa_erc_diff)

            # Increasing
            if((curr_psa_erc_final_avg - curr_psa_erc_initial_avg) >= 3):
                curr_psa_erc_trend = 'Increase'
                print_both('..PSA ERC TRENDING: ' + curr_psa_erc_trend + ' (UP ' + str(round(curr_psa_erc_diff_abs, 1)) + ')\r')

            # Decreasing
            if((curr_psa_erc_final_avg - curr_psa_erc_initial_avg) <= -3):
                curr_psa_erc_trend = 'Decrease'
                print_both('..PSA ERC TRENDING: ' + curr_psa_erc_trend + ' (DOWN ' + str(round(curr_psa_erc_diff_abs, 1)) + ')\r')

            # No Change
            if(curr_psa_erc_diff_abs < 3):
                curr_psa_erc_trend = 'No Change'
                if(curr_psa_erc_diff > 0):
                    print_both('..PSA ERC TRENDING: ' + curr_psa_erc_trend + ' (UP ' + str(round(curr_psa_erc_diff_abs, 1)) + ')\r')
                if(curr_psa_erc_diff < 0):
                    print_both('..PSA ERC TRENDING: ' + curr_psa_erc_trend + ' (DOWN ' + str(round(curr_psa_erc_diff_abs, 1)) + ')\r')
                if(curr_psa_erc_diff == 0):
                    print_both('..PSA ERC TRENDING: ' + curr_psa_erc_trend + ' (' + str(round(curr_psa_erc_diff_abs, 1)) + ')\r')
        else:
            # Set to NA
            curr_psa_erc_trend = pandas.NA
            print_both('..PSA ERC TRENDING: NOT ENOUGH OBSERVATIONS TO CALCULATE TREND\r')
        
        # Determine PSA ERC Forecast trend
        if( (len(curr_psa_erc_3day_fcast_initial_list) > 0) & (len(curr_psa_erc_3day_fcast_final_list) > 0) ):              
            # First, get PSA initial and final average ERC forecast (average of 1st day, and average of 3rd day)
            curr_psa_erc_fcast_initial_avg = statistics.mean(curr_psa_erc_3day_fcast_initial_list)
            curr_psa_erc_fcast_final_avg = statistics.mean(curr_psa_erc_3day_fcast_final_list)

            # Now determine the difference between the average 1st day, and average 3rd day
            curr_psa_erc_fcast_diff = curr_psa_erc_fcast_final_avg - curr_psa_erc_fcast_initial_avg
            curr_psa_erc_fcast_diff_abs = abs(curr_psa_erc_fcast_diff)

            # Increasing
            if((curr_psa_erc_fcast_final_avg - curr_psa_erc_fcast_initial_avg) >= 3):
                curr_psa_erc_fcast_trend = 'Increase'
                print_both('..PSA ERC FORECAST TRENDING: ' + curr_psa_erc_fcast_trend + ' (UP ' + str(round(curr_psa_erc_fcast_diff_abs, 1)) + ')\r')

            # Decreasing
            if((curr_psa_erc_fcast_final_avg - curr_psa_erc_fcast_initial_avg) <= -3):
                curr_psa_erc_fcast_trend = 'Decrease'
                print_both('..PSA ERC TRENDING: ' + curr_psa_erc_fcast_trend + ' (DOWN ' + str(round(curr_psa_erc_fcast_diff_abs, 1)) + ')\r')

            # No Change
            if(curr_psa_erc_fcast_diff_abs < 3):
                curr_psa_erc_fcast_trend = 'No Change'
                if(curr_psa_erc_fcast_diff > 0):
                    print_both('..PSA ERC FORECAST TRENDING: ' + curr_psa_erc_fcast_trend + ' (UP ' + str(round(curr_psa_erc_fcast_diff_abs, 1)) + ')\r')
                if(curr_psa_erc_fcast_diff < 0):
                    print_both('..PSA ERC FORECAST TRENDING: ' + curr_psa_erc_fcast_trend + ' (DOWN ' + str(round(curr_psa_erc_fcast_diff_abs, 1)) + ')\r')
                if(curr_psa_erc_fcast_diff == 0):
                    print_both('..PSA ERC FORECAST TRENDING: ' + curr_psa_erc_fcast_trend + ' (' + str(round(curr_psa_erc_fcast_diff_abs, 1)) + ')\r')
        else:
            # Set to NA
            curr_psa_erc_fcast_trend = pandas.NA
            print_both('..PSA ERC FORECAST TRENDING: NOT ENOUGH OBSERVATIONS TO CALCULATE TREND\r')

        # Insert the PSA average ERC Percentile, and Trend into 'psa_update_sdf' dataframe
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_percentile'] = curr_psa_avg_erc_percentile_round
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_trend'] = curr_psa_erc_trend
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_fcast_percentile'] = curr_psa_avg_erc_1day_fcast_percentile_round
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_fcast_trend'] = curr_psa_erc_fcast_trend


        ### BI ####

        # Calculate the PSA average BI Percentile
        if(len(curr_psa_bi_percentile_list) > 0):
            curr_psa_avg_bi_percentile = statistics.mean(curr_psa_bi_percentile_list)
            curr_psa_avg_bi_percentile_round = round(curr_psa_avg_bi_percentile, 2)
            print_both('..PSA BI PER MEAN: ' + str(curr_psa_avg_bi_percentile_round) + '\r')
        else:
            curr_psa_avg_bi_percentile_round = pandas.NA
            print_both('..PSA BI PER MEAN: NO OBSERVATIONS\r')

        # Calculate the PSA average BI Forecast Percentile
        if(len(curr_psa_bi_1day_fcast_percentile_list) > 0):
            curr_psa_avg_bi_1day_fcast_percentile = statistics.mean(curr_psa_bi_1day_fcast_percentile_list)
            curr_psa_avg_bi_1day_fcast_percentile_round = round(curr_psa_avg_bi_1day_fcast_percentile, 2)
            print_both('..PSA BI PER FORECAST MEAN: ' + str(curr_psa_avg_bi_1day_fcast_percentile_round) + '\r')
        else:
            curr_psa_avg_bi_1day_fcast_percentile_round = pandas.NA
            print_both('..PSA BI PER FORECAST MEAN: NO OBSERVATIONS\r')
            
        # Determine PSA BI trend
        if( (len(curr_psa_bi_initial_list) > 0) & (len(curr_psa_bi_final_list) > 0) ):
            # First, get PSA initial and final average BI (average of 1st day, and average of 3rd day)
            curr_psa_bi_initial_avg = statistics.mean(curr_psa_bi_initial_list)
            curr_psa_bi_final_avg = statistics.mean(curr_psa_bi_final_list)

            # Now determine the difference between the average 1st day, and average 3rd day
            curr_psa_bi_diff = curr_psa_bi_final_avg - curr_psa_bi_initial_avg
            curr_psa_bi_diff_abs = abs(curr_psa_bi_diff)

            # Increasing
            if((curr_psa_bi_final_avg - curr_psa_bi_initial_avg) >= 3):
                curr_psa_bi_trend = 'Increase'
                print_both('..PSA BI TRENDING: ' + curr_psa_bi_trend + ' (UP ' + str(round(curr_psa_bi_diff_abs, 1)) + ')\r')

            # Decreasing
            if((curr_psa_bi_final_avg - curr_psa_bi_initial_avg) <= -3):
                curr_psa_bi_trend = 'Decrease'
                print_both('..PSA BI TRENDING: ' + curr_psa_bi_trend + ' (DOWN ' + str(round(curr_psa_bi_diff_abs, 1)) + ')\r')

            # No Change
            if(curr_psa_bi_diff_abs < 3):
                curr_psa_bi_trend = 'No Change'
                if(curr_psa_bi_diff > 0):
                    print_both('..PSA BI TRENDING: ' + curr_psa_bi_trend + ' (UP ' + str(round(curr_psa_bi_diff_abs, 1)) + ')\r')
                if(curr_psa_bi_diff < 0):
                    print_both('..PSA BI TRENDING: ' + curr_psa_bi_trend + ' (DOWN ' + str(round(curr_psa_bi_diff_abs, 1)) + ')\r')
                if(curr_psa_bi_diff == 0):
                    print_both('..PSA BI TRENDING: ' + curr_psa_bi_trend + ' (' + str(round(curr_psa_bi_diff_abs, 1)) + ')\r')
        else:
            # Set to NA
            curr_psa_bi_trend = pandas.NA
            print_both('..PSA BI TRENDING: NOT ENOUGH OBSERVATIONS TO CALCULATE TREND\r')
            
        # Determine PSA BI Forecast trend
        if( (len(curr_psa_bi_3day_fcast_initial_list) > 0) & (len(curr_psa_bi_3day_fcast_final_list) > 0) ):
            # First, get PSA initial and final average BI forecast (average of 1st day, and average of 3rd day)
            curr_psa_bi_fcast_initial_avg = statistics.mean(curr_psa_bi_3day_fcast_initial_list)
            curr_psa_bi_fcast_final_avg = statistics.mean(curr_psa_bi_3day_fcast_final_list)

            # Now determine the difference between the average 1st day, and average 3rd day
            curr_psa_bi_fcast_diff = curr_psa_bi_fcast_final_avg - curr_psa_bi_fcast_initial_avg
            curr_psa_bi_fcast_diff_abs = abs(curr_psa_bi_fcast_diff)

            # Increasing
            if((curr_psa_bi_fcast_final_avg - curr_psa_bi_fcast_initial_avg) >= 3):
                curr_psa_bi_fcast_trend = 'Increase'
                print_both('..PSA BI FORECAST TRENDING: ' + curr_psa_bi_fcast_trend + ' (UP ' + str(round(curr_psa_bi_fcast_diff_abs, 1)) + ')\r')

            # Decreasing
            if((curr_psa_bi_fcast_final_avg - curr_psa_bi_fcast_initial_avg) <= -3):
                curr_psa_bi_fcast_trend = 'Decrease'
                print_both('..PSA BI TRENDING: ' + curr_psa_bi_fcast_trend + ' (DOWN ' + str(round(curr_psa_bi_fcast_diff_abs, 1)) + ')\r')

            # No Change
            if(curr_psa_bi_fcast_diff_abs < 3):
                curr_psa_bi_fcast_trend = 'No Change'
                if(curr_psa_bi_fcast_diff > 0):
                    print_both('..PSA BI FORECAST TRENDING: ' + curr_psa_bi_fcast_trend + ' (UP ' + str(round(curr_psa_bi_fcast_diff_abs, 1)) + ')\r')
                if(curr_psa_bi_fcast_diff < 0):
                    print_both('..PSA BI FORECAST TRENDING: ' + curr_psa_bi_fcast_trend + ' (DOWN ' + str(round(curr_psa_bi_fcast_diff_abs, 1)) + ')\r')
                if(curr_psa_bi_fcast_diff == 0):
                    print_both('..PSA BI FORECAST TRENDING: ' + curr_psa_bi_fcast_trend + ' (' + str(round(curr_psa_bi_fcast_diff_abs, 1)) + ')\r')

            # Insert the PSA average BI Percentile, and Trend into 'psa_update_sdf' dataframe
            psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_percentile'] = curr_psa_avg_bi_percentile_round
            psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_trend'] = curr_psa_bi_trend
            psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_fcast_percentile'] = curr_psa_avg_bi_1day_fcast_percentile_round
            psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_fcast_trend'] = curr_psa_bi_fcast_trend
        else:
            # Set to NA
            curr_psa_bi_fcast_trend = pandas.NA
            print_both('..PSA BI FORECAST TRENDING: NOT ENOUGH OBSERVATIONS TO CALCULATE TREND\r')
            
        # Insert update date
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'nfdr_dt'] = datetime_today.strftime('%m/%d/%Y')
        
    except Exception as e:

        print_both('.ERROR:\r')
        print_both(str(e))
        print_both('\r')
        print_both('.INSERTING NULL VALUES INTO PSA UPDATE DATAFRAME\r')

        # Insert NA into the PSA fields
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_percentile'] = pandas.NA
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_fcast_percentile'] = pandas.NA
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_trend'] = pandas.NA
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_ec_fcast_trend'] = pandas.NA
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_percentile'] = pandas.NA
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_fcast_percentile'] = pandas.NA
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_trend'] = pandas.NA
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'avg_bi_fcast_trend'] = pandas.NA
        
        # Insert update date
        psa_update_sdf.loc[(psa_update_sdf['PSANationalCode'] == PSAs[i]), 'nfdr_dt'] = pandas.NA


#####################################################################################################
### UPDATE SERVICE
#####################################################################################################

# Set the timezone for field 'nfdr_datetime' in raws_update_sdf
raws_nfdr_datetime = pandas.to_datetime(raws_update_sdf['nfdr_datetime'])
raws_nfdr_datetime_tzaware = raws_nfdr_datetime.dt.tz_localize('America/Los_Angeles')
raws_update_sdf['nfdr_datetime'] = raws_nfdr_datetime_tzaware

# Set the timezone for field 'obs_datetime' in raws_update_sdf
raws_obs_datetime = pandas.to_datetime(raws_update_sdf['obs_datetime'])
raws_obs_datetime_tzaware = raws_obs_datetime.dt.tz_localize('America/Los_Angeles')
raws_update_sdf['obs_datetime'] = raws_obs_datetime_tzaware

# Fill NAs in dataframes with values of None, else will throw an error when updating service
raws_update_sdf = raws_update_sdf.replace({numpy.nan: None})
psa_update_sdf = psa_update_sdf.replace({numpy.nan: None})

# Update the feature service with the new data
print_both('\r')
print_both('UPDATING FEATURES\r')

print_both('.RAWS\r')
raws_upload = False
for i in range(0,5): # Try update up to 5 times
    try:
        raws_update_fset = arcgis.features.FeatureSet.from_dataframe(raws_update_sdf)
        raws_layer.edit_features(updates = raws_update_fset)
        raws_upload = True
    except:
        pass
    if raws_upload == False:
        print_both('..UPLOAD FAILED, RE-TRYING\r')
        sleep(30) # Wait 30 seconds before trying again
    else:
        break
if raws_upload == False:
    print_both('..RAWS FAILED TO UPDATE AFTER 5 ATTEMPTS\r')

print_both('.PSA\r')
GACCs = sorted(list(set(psa_update_sdf['GACC'].tolist())))
for i in range(0, len(GACCs)):
    print_both('..Updating ' + GACCs[i])
    psa_upload = False
    for j in range(0,5): # Try update up to 5 times
        try:
            ga_sdf = psa_update_sdf.loc[psa_update_sdf['GACC'] == GACCs[i],]
            psa_update_fset = arcgis.features.FeatureSet.from_dataframe(ga_sdf)
            psa_layer.edit_features(updates = psa_update_fset)
            psa_upload = True
        except:
            pass
        if psa_upload == False:
            print_both('...UPLOAD FAILED, RE-TRYING\r')
            sleep(30) # Wait 30 seconds before trying again
        else:
            break
    if psa_upload == False:
        print_both('...PSA FAILED TO UPDATE AFTER 5 ATTEMPTS\r')
        
# Save data for troubleshooting
raws2psa_df.to_csv(wdir + '/raws2psa_data.csv')
raws_update = raws_update_sdf.drop('SHAPE',axis=1)
raws_update.to_csv(wdir + '/raws_data.csv')
psa_update = psa_update_sdf.drop('SHAPE',axis=1)
psa_update.to_csv(wdir + '/psa_data.csv')

print_both('\r')
print_both('DONE!\r')
print_both('\r')

# Close log file
lf.close()

