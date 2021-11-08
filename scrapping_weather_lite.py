import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from bs4 import BeautifulSoup
import requests
from datetime import date, datetime, timedelta, timezone
from lxml import etree
import re
import unidecode
import shutil
import logging
from os import listdir
from os.path import isfile, join

def get_data(div):
    """
    Function that extract the data of day and temp (high and low)

    Input
    -------------
        :param div: section of the web that contains the information of interest date, high and low
    
    Output
    -------------
        :param day: Return string with the day of the data extracted data
        :param temp_high: Return string with the temp_high of the data extracted data 
        :param temp_low: Return string with the temp_low of the data extracted data

    """
    day = div.find(class_="date").get_text().strip()
    if(div.find(class_="high")):
        temp_high = div.find(class_="high").get_text().strip().replace('°','')
    if(div.find(class_="low")):
        temp_low = div.find(class_="low").get_text().strip().replace('°','')   
    return day,temp_high, temp_low

def get_date(day,months,month,year):
    """
    Function that return the day validated (the correct format of the day)
    selected

    Input
    -------------
        :param day: string with the day
        :param months: Serie with the number and name of the mont
        :param month: string with the month
        :param year: string with the year

     Output
    -------------
        :param t_date: Return string with day format YYYY-MM-DD
  
    """
    t_day = day.split('/')[0]
    t_month = str(list(months.keys()).index(month)+1)
    if("/" in (day)):
        t_month = day.split('/')[0]
        t_day = day.split('/')[1]
    t_date=""
    try:
        t_date = date(int(year),int(t_month),int(t_day)).isoformat()
        return t_date
    except ValueError:
        print("Incorrect Date")

def load_data(years = [2021],
              url_base = 'https://www.accuweather.com/en/',
              prefix_file_name = 'Accuweather_Hotel_',
              medium_file_name= '_Weather_',
              webs_scrapping = {
                    '24':'https://www.accuweather.com/en/es/guia-deisora/38680/march-weather/114904_pc',
                    '148':'https://www.accuweather.com/en/mx/cancun-centro/77500/march-weather/224874_pc',
                    '229':'https://www.accuweather.com/en/es/malaga/29002/february-weather/96272_pc',
                    '285':'https://www.accuweather.com/en/gt/guatemala-city/187765/march-weather/187765',
                    '410':'https://www.accuweather.com/en/it/roma/00147/march-weather/190688_pc',
                    '445':'https://www.accuweather.com/en/es/guia-deisora/38680/march-weather/114904_pc',
                    '473':'https://www.accuweather.com/en/aw/salina-cerca/1-12069_1_al/march-weather/1-12069_1_al',
                    '474':'https://www.accuweather.com/en/mx/nuevovallarta/3578871/march-weather/3578871',
                    '483':'https://www.accuweather.com/en/do/santo-domingo/125887/march-weather/125887',
                    '85':'https://www.accuweather.com/en/cr/san-jose/1-115295_1_al/december-weather/1-115295_1_al',
                    '86':'https://www.accuweather.com/en/cr/tambor/1179307/weather-forecast/1179307',
                    '476':'https://www.accuweather.com/en/cr/liberia/112804/weather-forecast/112804',
                    '92':'https://www.accuweather.com/en/cr/tamarindo/1179309/december-weather/1179309',
                    '86':'https://www.accuweather.com/en/cr/tambor/1179307/weather-forecast/1179307',
                    '533':'https://www.accuweather.com/en/mx/santiago-de-quer%C3%A9taro/3583792/december-weather/3583792',
                    '212':'https://www.accuweather.com/en/mx/canc%C3%BAn/1-235049_1_al/december-weather/1-235049_1_al',
                    '213':'https://www.accuweather.com/en/do/punta-cana/462477/december-weather/462477',
                    '543':'https://www.accuweather.com/en/mx/mexico-city/1-242560_1_al/weather-forecast/1-242560_1_al'
                    },
              months = {"1":'january',"2":'february',"3":'march',"4":'april',"5":'may',"6":'june',"7":'july',"8":'august',"9":'september',"10":'october',"11":'november',"12":'december'},
              output = './output/data/'):
    """
    Function that load the data for diferent years and a list of relevant zones, generated independient, and generates a CSVs with the information of temp_high and temp_low by day in the years
    selected

    Input
    -------------
        :param years: array of years to extract information
        :param url_base: string with the base of the ul to launch scraper
        :param prefix_file_name: string with prefix for file name tha will generate with the results
        :param medium_file_name: string with  medium name for file name tha will generate with the results
        :param webs_scrapping: serie with the webs to scrap, to be simple permits all the url and the process extract the information relevant to create the correct url
        :param months: serie with the number and the name of the month, the URL that will need to generate use the name of the month
        :param output: path base to save the CSVs with the results
  
    """
    
    
    #The use of headers help us to avoid connect refuse
    headers = {
    'User-Agent': "PostmanRuntime/7.15.0",
    'Accept': "*/*",
    'Cache-Control': "no-cache",
    'accept-encoding': "gzip, deflate",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
    }
    data = {}
    
    #It will creates the output path if not exist it
    if os.path.exists(output):
        shutil.rmtree(output)
    os.makedirs(output)
    df_all = pd.DataFrame()

    #Loop to create all info from webs to scrap
    for web in webs_scrapping:
        web_splited = webs_scrapping.get(web).split('/')
        hotel_id = web
        cod_country = web_splited[4]
        province = web_splited[5]
        zip_code = web_splited[6]
        url_end =web_splited[8]
        for year in years: #This help us to create the historical data
            for month in months:
                logging.info(f"web: {web} year {year} month {month}")
                url = url_base + cod_country + '/' + province + '/' + zip_code + '/' + months.get(month) + '-weather/'+url_end+'?year='+ str(year)
                html_doc = requests.request("GET", url, headers=headers).text
                soup = BeautifulSoup(html_doc, 'lxml')
                # The class that contains the relevant information about the weather (temperature)
                # Before current day we hace to use monthly-daypanel is-past
                # Current day monthly-daypanel is-today
                # Future monthly-daypanel
                divs = soup.find_all(True, {'class':['monthly-daypanel is-past', 'monthly-daypanel is-today', 'monthly-daypanel']})
                for div in divs:
                    day,temp_high,temp_low = get_data(div)
                    t_date = get_date(day,months,month,year)
                    data[t_date] = (t_date,temp_high, temp_low, cod_country,province,zip_code)
        
        df_weather = pd.DataFrame.from_dict(data, orient='index', columns=['t_date','temp_high','temp_low','cod_country','province','zip_code'])
        df_weather = df_weather.reset_index(drop=True)
        file_name = prefix_file_name + str(hotel_id) + medium_file_name + province + '.csv'
        df_weather.to_csv(output + '/' + file_name, index=False)
        df_all = pd.concat([df_all,df_weather])
    df_all.to_csv(output + '/' + "WeatherForecast.csv" ,index=False)

if __name__ == "__main__":
    load_data()
