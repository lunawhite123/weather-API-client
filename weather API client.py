import asyncio
import aiohttp
import sqlite3
import matplotlib.pyplot as plt
import os
import logging
from datetime import datetime,timedelta
from rich import print
from rich.console import Console
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

handler = RotatingFileHandler(
    'weather.log',
    maxBytes=1024*64,
    backupCount=3,
    encoding='utf-8'
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H-%M-%S',
    handlers = [handler])
    



load_dotenv('api.env')
console = Console()

cities = input("Enter city or cities (comma separated): ").split(',')
celsius_or_fahrenheit = input("Enter degree metric (C, F): ").lower().strip()
db_path = os.getenv('db_path')
API = os.getenv('API_KEY')

def init_db(db_path):
    with sqlite3.connect(db_path) as db:
        cursor = db.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS weather (
                        city TEXT PRIMARY KEY,
                        temp REAL,
                        wind REAL,
                        humidity INTEGER,
                        timestamp DATETIME)""")
init_db(db_path)

def check_degree(degree):
    if degree == 'c':
        return 'metric'
    else:
        return 'imperial'

async def fetch_data(city,celsius_or_fahrenheit):
    metric = check_degree(celsius_or_fahrenheit)
    url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API}&units={metric}'
    city = city.strip()
    
    database = sqlite3.connect(db_path)
    cursor = database.cursor()
    
    try:
        cursor.execute("SELECT * FROM weather WHERE city = ?", (city,))
        weather = cursor.fetchone()
        if weather:
            timestamp = datetime.strptime(weather[4], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - timestamp < timedelta(hours=1):
                logging.info(f"city: {city}, temp: {weather[1]}, humidity: {weather[3]}, wind: {weather[2]}, source: cache")
                
                return {
                    'city': city, 
                    'temp': weather[1],
                    'wind': weather[2],
                    'humidity': weather[3],
                    'source': 'cache'
                }
        
        async with aiohttp.ClientSession() as sesh:
            async with sesh.get(url) as resp:
                if resp.status != 200:
                    return f"city {city} ERROR, {resp.status}"
                data = await resp.json()
                cursor.execute("""INSERT OR REPLACE INTO weather VALUES (?, ?, ?, ?, ?)""",
                              (city, data['main']['temp'], data['wind']['speed'], 
                               data['main']['humidity'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                database.commit()
                
                logging.info(f"city: {city}, temp: {data['main']['temp']}, humidity: {data['main']['humidity']}, wind: {data['wind']['speed']}, source: API")
                
                return {
                    'city': city,
                    'temp': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'wind': data['wind']['speed'],
                    'source': 'API'
                }
    
    except Exception as e:
        return f"Error for {city}: {str(e)}"
    finally:
        cursor.close()
        database.close()
    
async def fetch_all(cities):
    tasks = [fetch_data(city) for city in cities]
    data = await asyncio.gather(*tasks)
    return '\n'.join(str(i) for i in data)

async def get_3day_forecast(city, celsius_or_fahrenheit):
    metric = check_degree(celsius_or_fahrenheit)
    url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API}&units={metric}'
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError("Wrong city")
            data = await response.json()
            return parse_3day_forecast(data)
            

def parse_3day_forecast(data):
    forecast = {}
    today = datetime.now().date()
    days_collected = 0

    for item in data['list']:
        dt = datetime.strptime(item['dt_txt'], '%Y-%m-%d %H:%M:%S')
        time_str = dt.strftime('%H:%M')
        if time_str not in ('21:00', '12:00'):
            continue
        
        date = dt.date()
        if date <= today or days_collected >= 3:
            continue

        if not date in forecast:
            forecast[date] = []
            days_collected += 1
            
        forecast[date].append({
            'time': item['dt_txt'].split()[1][:5],
            'temp': item['main']['temp'],
            'description': item['weather'][0]['description'],
            'icon': item['weather'][0]['icon']})
        
        logging.info(f"time: {item['dt_txt'].split()[1][:5]}, temp: {item['main']['temp']}, description: {item['weather'][0]['description']}, icon: {item['weather'][0]['icon']}")
    
    return forecast
        
async def main(cities):
    plt.figure(figsize=(10,5))
    if celsius_or_fahrenheit == 'c':
        degree = '°С'
    else:
        degree = '℉'
    
    forecast_for_graph = {}
    forecasts = {}
    for city in cities:
        city = city.strip()
        try:
            city_forecast = await get_3day_forecast(city, celsius_or_fahrenheit)
            forecasts[city] = city_forecast
        
        except ValueError as e:
            print(f"\nОшибка для города {city}: {str(e)}")
            continue
    
    for city, city_forecast in forecasts.items():
        console.print(f'\n[blue]Forecast for {city}[/blue]', style='italic magenta on black')
        for date, day_data in city_forecast.items():
                
                print(f"\n{date.strftime('%A, %d %B')}:")
                for timepoint in day_data:
                    forecast_for_graph[date] = timepoint['temp']
                    console.print(f"[italic]{timepoint['time']}: {timepoint['temp']} {degree}, {timepoint['description']}")    
    
    plt.plot(forecast_for_graph.keys(), forecast_for_graph.values(), marker='o', label='temperature', color='red', linewidth=2)
    plt.title('Forecast for 3 days')
    plt.xlabel('Dates')
    plt.ylabel('Temperature')
    plt.legend()
    plt.savefig("temperature_plot.png", dpi=500)
    
async def get_1day(cities):
    if len(cities) == 1:
        res = await fetch_data(cities[0], celsius_or_fahrenheit)
    else:
        res = await fetch_all(cities)
    return res

asyncio.run(main(cities))

