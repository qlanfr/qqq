from datetime import datetime
import fear_and_greed
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os


load_dotenv()

# 공포와 탐욕 지수 가져오는 함수
def get_fr_gd():
    fg = fear_and_greed.get()
    fg_sc = float(fg[0])
    fg_sc = round(fg_sc, 2)
    fg_st = fg[1]
    return (fg_sc, fg_st)


def save_to_db(score, status):
    connection = None
    try:
       
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        # MySQL 연결 설정
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )

        cursor = connection.cursor()

        
        today = datetime.now().strftime("%Y-%m-%d")

        
        sql = "INSERT INTO fear_db (date, fear_greed_score, status) VALUES (%s, %s, %s)"
        values = (today, score, status)

        
        cursor.execute(sql, values)
        connection.commit()

        print(f"Data saved: {today}, {score}, {status}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 공포와 탐욕 지수 가져오기 및 DB 저장
fg_score, fg_status = get_fr_gd()
save_to_db(fg_score, fg_status)

