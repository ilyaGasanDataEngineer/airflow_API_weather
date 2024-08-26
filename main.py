from os import getcwd

from datetime import datetime
from traceback import print_tb
from urllib.parse import urljoin

import pandas
class ETL_data30():
    def getting_data(self):
        import xmltodict
        import requests

        url = 'https://api.worldweatheronline.com/premium/v1/weather.ashx?key=45b084f9b45d4deca56125148242608&q=61.783673,%2034.328678'

        response = requests.get(url)

        response_json = xmltodict.parse(response.text)
        data = response_json['data']['weather']
        data_list = []
        for date in data:
            data_to_send = dict()
            data_to_send['windspeedKmph'] = list()
            data_to_send['visibility'] =  list()
            data_to_send['tempC'] =  list()
            data_to_send['time'] =  list()

            data_to_send['date_request'] = datetime.now().day
            data_to_send['min_temp'] = date['mintempC']
            data_to_send['max_temp'] = date['maxtempC']
            data_to_send['uv_Index'] = date['uvIndex']
            for time in date['hourly']:
                print(type(data_to_send['windspeedKmph']))
                data_to_send['windspeedKmph'].append(time['windspeedKmph'])
                data_to_send['visibility'].append(time['visibility'])
                data_to_send['tempC'].append(time['tempC'])
                data_to_send['time'].append(time['time'])
            data_to_send['date_interested'] = date['date']
            data_list.append(data_to_send)
            print(data_to_send, end='\n\n\n\n\n\n\n')
    def push_data(self):
        pass



etl = ETL_data30()
etl.getting_data()