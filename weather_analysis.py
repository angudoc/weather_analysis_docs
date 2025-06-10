import requests
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient

# === 1. Получение данных о погоде ===
def get_weather_data(city, latitude, longitude):
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={latitude}&longitude={longitude}"
        f"&hourly=temperature_2m"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
        f"&timezone=auto"
    )

    response = requests.get(url)
    data = response.json()

    if 'hourly' not in data:
        raise ValueError(f"Ошибка при получении данных для {city}: {data}")

    df = pd.DataFrame(data['hourly'])
    df['city'] = city
    return df[['time', 'temperature_2m', 'city']]

# === 2. Сбор данных по городам === 
cities = {
    "Moscow": {"lat": 55.7558, "lon": 37.6173},
    "London": {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Tokyo": {"lat": 35.6895, "lon": 139.6917}
}

all_data = []
for city, coords in cities.items():
    print(f"Загрузка данных для {city}...")
    df = get_weather_data(city, coords['lat'], coords['lon'])
    all_data.append(df)

weather_df = pd.concat(all_data, ignore_index=True)
weather_df.to_csv("weather_data.csv", index=False)
print("Данные сохранены в файл weather_data.csv")

# === 3. Визуализация ===
# График изменения температуры
plt.figure(figsize=(14, 6))
sns.lineplot(data=weather_df, x='time', y='temperature_2m', hue='city', marker="o", linewidth=1.5)
plt.xticks(rotation=45)
plt.title('Изменение температуры за последние 30 дней')
plt.xlabel('Дата')
plt.ylabel('Температура (°C)')
plt.tight_layout()
plt.savefig("temperature_trend.png")
plt.show()

# Распределение температуры
plt.figure(figsize=(10, 6))
sns.histplot(data=weather_df, x='temperature_2m', hue='city', bins=30, kde=True)
plt.title('Распределение температур')
plt.xlabel('Температура (°C)')
plt.ylabel('Частота')
plt.tight_layout()
plt.savefig("temperature_distribution.png")
plt.show()

# === 4. Работа с HDFS ===
client = InsecureClient('http://localhost:9870')  # замените на ваш адрес
client.makedirs('/user/airflow/weather_data')
client.upload('/user/airflow/weather_data/weather_data.csv', 'weather_data.csv')
print("Файл загружен в HDFS")

# === 5. Выгрузка из HDFS ===
client.download('/user/airflow/weather_data/weather_data.csv', 'downloaded_weather_data.csv')
print("Файл успешно выгружен из HDFS")
