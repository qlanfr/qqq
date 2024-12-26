import requests
import pymysql
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os


load_dotenv()


db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}


FRED_API_KEY = os.getenv('FRED_API_KEY')


START_DATE = os.getenv('START_DATE')
END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def save_to_mysql(data, table_name, column_name):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    for date, value in data.items():
        cursor.execute(f"""
            INSERT INTO {table_name} (Date, {column_name})
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                {column_name}=VALUES({column_name})
        """, (date, value))

    connection.commit()
    connection.close()


def fetch_fred_data(series_id, start_date, end_date):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&start_date={start_date}&end_date={end_date}"
    response = requests.get(url)
    data = response.json().get('observations', [])

    
    min_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    result = {}
    for item in data:
        date = datetime.strptime(item['date'], "%Y-%m-%d")
        if date >= min_date:
            try:
                value = float(item['value'])
                result[date] = value
            except ValueError:
                continue  

    return result

# CPI 데이터 저장
def fetch_and_save_cpi(start_date, end_date):
    cpi_data = fetch_fred_data('CPIAUCSL', start_date, end_date)
    save_to_mysql(cpi_data, 'cpi_db', 'CPI')
    print("CPI 데이터가 MySQL에 저장되었습니다.")

# PPI 데이터 저장
def fetch_and_save_ppi(start_date, end_date):
    ppi_data = fetch_fred_data('PPIACO', start_date, end_date)
    save_to_mysql(ppi_data, 'ppi_db', 'PPI')
    print("PPI 데이터가 MySQL에 저장되었습니다.")

# 실업률 데이터 저장
def fetch_and_save_unemployment(start_date, end_date):
    unemployment_data = fetch_fred_data('UNRATE', start_date, end_date)
    save_to_mysql(unemployment_data, 'work_db', 'Unemployment_Rate')
    print("실업률 데이터가 MySQL에 저장되었습니다.")

# 미국 금리 데이터 저장 (1년, 5년, 10년, 30년)
def fetch_and_save_interest_rates(start_date, end_date):
    rate_1y = fetch_fred_data('DGS1', start_date, end_date)
    rate_5y = fetch_fred_data('DGS5', start_date, end_date)
    rate_10y = fetch_fred_data('DGS10', start_date, end_date)
    rate_30y = fetch_fred_data('DGS30', start_date, end_date)

    
    dates = set(rate_1y.keys()).union(rate_5y.keys(), rate_10y.keys(), rate_30y.keys())
    rates_data = {date: (rate_1y.get(date), rate_5y.get(date), rate_10y.get(date), rate_30y.get(date)) for date in dates}

    
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    for date, rates in rates_data.items():
        cursor.execute("""
            INSERT INTO us_db (Date, Rate_1Y, Rate_5Y, Rate_10Y, Rate_30Y)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Rate_1Y=VALUES(Rate_1Y),
                Rate_5Y=VALUES(Rate_5Y),
                Rate_10Y=VALUES(Rate_10Y),
                Rate_30Y=VALUES(Rate_30Y)
        """, (date, rates[0], rates[1], rates[2], rates[3]))

    connection.commit()
    connection.close()
    print("미국 금리 데이터가 MySQL에 저장되었습니다.")


if __name__ == "__main__":
    
    fetch_and_save_cpi(START_DATE, END_DATE)
    fetch_and_save_ppi(START_DATE, END_DATE)
    fetch_and_save_unemployment(START_DATE, END_DATE)
    fetch_and_save_interest_rates(START_DATE, END_DATE)

    
    today = datetime.now().strftime("%Y-%m-%d")
    fetch_and_save_cpi(today, today)
    fetch_and_save_ppi(today, today)
    fetch_and_save_unemployment(today, today)
    fetch_and_save_interest_rates(today, today)
    print("오늘의 경제 지표 데이터가 MySQL에 저장되었습니다.")

