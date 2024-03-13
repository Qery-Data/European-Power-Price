from entsoe import EntsoePandasClient
import pandas as pd
import datetime
import requests
import os
access_token = os.getenv('ENTSOE_TOKEN')


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
current_date = pd.Timestamp(datetime.datetime.now().date(), tz='Europe/Brussels')
start = current_date
end = current_date + pd.Timedelta(days=1)
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
csv_file_path = 'data/DA_prices_converted_to_NOK.csv'
Day_Prices.to_csv(csv_file_path, index=False)