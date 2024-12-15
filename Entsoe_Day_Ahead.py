from entsoe import EntsoePandasClient
import pandas as pd
import datetime
import requests
import os
from pytz import timezone
from datawrapper import Datawrapper
access_token = os.getenv('ENTSOE_TOKEN')
access_token_DW = os.getenv('DW_TOKEN')
dw = Datawrapper(access_token=access_token_DW)
os.makedirs('data', exist_ok=True)

# Get latest exchange rate from Norges Bank
url = "https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP?format=sdmx-json&lastNObservations=1&locale=en"
response = requests.get(url)
data = response.json()
series = data['data']['dataSets'][0]['series']
series_key = next(iter(series))
observations = series[series_key]['observations']
exchange_rate = next(iter(observations.values()))[0]
exchange_rate = float(exchange_rate)

# Initialize Entsoe Client
client = EntsoePandasClient(api_key=access_token)
oslo_tz = timezone('Europe/Oslo')
current_date = pd.Timestamp(datetime.datetime.now(oslo_tz).date(), tz='Europe/Oslo')
tomorrow = current_date + pd.Timedelta(days=1)
start = pd.Timestamp(tomorrow, tz='Europe/Oslo')  
end = start + pd.Timedelta(days=1)  
formatted_date = tomorrow.strftime('%d/%m/%Y')
codes = ['NO_1', 'NO_2', 'NO_3', 'NO_4', 'NO_5']
Day_Prices = pd.DataFrame()

# Loop through each area and query the prices
for code in codes:
    prices = client.query_day_ahead_prices(code, start=start, end=end).to_frame().reset_index()
    prices.columns = ['Time', f'Price_{code}']
    if Day_Prices.empty:
        Day_Prices = prices
    else:
        Day_Prices = pd.merge(Day_Prices, prices, on='Time', how='outer')

# Convert prices from EUR/MWh to NOK/kr/kWh and add 25% VAT
for code in codes:
    if code == 'NO_4':  # NO_4 is exempt from VAT
        Day_Prices[f'Price_{code}'] = (Day_Prices[f'Price_{code}'] * exchange_rate) / 1000
    else:  # Apply 25% VAT to other areas
        Day_Prices[f'Price_{code}'] = ((Day_Prices[f'Price_{code}'] * exchange_rate) / 1000) * 1.25

# Convert Time to the desired format without seconds (YYYY-MM-DD HH:MM:SS)
Day_Prices['Time'] = Day_Prices['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Save the DataFrame to a CSV file
csv_file_path = 'data/Day_Ahead_Prices_Norway.csv'
Day_Prices.to_csv(csv_file_path, index=False)

#Update DW charts
dw.update_chart(chart_id='hNWmM', title='<b style="font-size:17px">Strømpris i Øst (NO1) ' + formatted_date +'</b>')
dw.publish_chart(chart_id='hNWmM', display=False)
dw.update_chart(chart_id='TegsW', title='<b style="font-size:17px">Strømpris i Sør (NO2) ' + formatted_date +'</b>')
dw.publish_chart(chart_id='TegsW', display=False)
dw.update_chart(chart_id='jMqlH', title='<b style="font-size:17px">Strømpris i Midt (NO3) ' + formatted_date +'</b>')
dw.publish_chart(chart_id='jMqlH', display=False)
dw.update_chart(chart_id='CVUb6', title='<b style="font-size:17px">Strømpris i Nord (NO4) ' + formatted_date +'</b>')
dw.publish_chart(chart_id='CVUb6', display=False)
dw.update_chart(chart_id='wDNoM', title='<b style="font-size:17px">Strømpris i Vest (NO5) ' + formatted_date +'</b>')
dw.publish_chart(chart_id='wDNoM', display=False)