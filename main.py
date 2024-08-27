from os import getcwd

from datetime import datetime
from os.path import curdir
from select import select
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
                data_to_send['windspeedKmph'].append(time['windspeedKmph'])
                data_to_send['visibility'].append(time['visibility'])
                data_to_send['tempC'].append(time['tempC'])
                data_to_send['time'].append(time['time'])
            data_to_send['date_interested'] = date['date']
            data_list.append(data_to_send)
        return data_list

    def data_transform(self, data):
        for i in range(len(data)):
            data[i]['date_request'] = datetime.strptime(f"2024-08-{data[i]['date_request']}", "%Y-%m-%d").date()
        return data
    def push_data(self, data:list):
        import psycopg2
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="29892989",
                host="localhost",
                port="5432"
            )
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO api_data_weather (date_request, date_interested, min_temp, max_temp, uv_Index, windspeedKmph, visibility, tempC, time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            for record in data:
                date_request = record['date_request']
                for i in range(len(record['time'])):
                    print(data)
                    cursor.execute(insert_query, (
                        date_request,
                        record['date_interested'],  # Дата уже в правильном формате
                        float(record['min_temp']),
                        float(record['max_temp']),
                        int(record['uv_Index']),
                        float(record['windspeedKmph'][i]),
                        float(record['visibility'][i]),
                        float(record['tempC'][i]),
                        float(record['time'][i])
                    ))

            conn.commit()

            # Закрытие соединения
            cursor.close()
            conn.close()

        except Exception as e:
            print(f"Ошибка при подключении к базе данных: {e}")
    def get_data_from_ps(self):
        import psycopg2
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="29892989",
                host="localhost",
                port="5432"
            )
            cursor = conn.cursor()
            select_query = """
            select distinct * from   api_data_weather count order by date_interested;
            """
            cursor.execute(select_query)
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        except Exception as e:
            print(f'error {e}')


etl = ETL_data30()
#data = etl.getting_data()
#etl.push_data(data)
#data = etl.data_transform(data)
#etl.push_data(data)
etl.get_data_from_ps()