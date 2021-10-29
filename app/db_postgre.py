import psycopg2
import os


PASSWORD = os.getenv("PASSWORD_P")

# Подключение базы данных
def connect_db():
    return psycopg2.connect(user='postgres',
                            password=PASSWORD,
                            host='127.0.0.1',
                            port='5432',
                            database="weather_db")


def select_last_info(table):
    """ Выбока последних 15 записей """
    result = []
    conn = connect_db()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT temperature, time, view_date, descrip, created "
                       f"FROM {table} "
                       f"ORDER BY created DESC "
                       f"LIMIT 15 ")
        data = cursor.fetchall()
        for i in data:
            result.append({'temp': i[0],
                           'time': i[1],
                           'view_date': i[2],
                           'descrip': i[3],
                           'created': i[4],
                           })
    except conn.Error as error:
        print("Error connection to database", error)
    finally:
        if conn:
            conn.close()
    return result


def select_max_min(table):
    """ Максимальная и Минимальная за день """
    result = []
    conn = connect_db()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT temperature, time, descrip, view_date "
                       f"FROM {table} "
                       f"WHERE created >= 'today'::timestamp "
                       f"ORDER BY temperature DESC;")
        data = cursor.fetchall()
        for i in data:
            result.append({'temp': i[0],
                           'time': i[1],
                           'descrip': i[2],
                           'view_date': i[3],
                           })
    except conn.Error as error:
        print("Error connection to database", error)
    finally:
        if conn:
            conn.close()
    return result


def select_avg_temp():
    """ Средняя за час с rp5_ru по всем улицам"""
    result = ''
    conn = connect_db()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT round(AVG(temperature), 2) "
                       f"FROM rp5_ru "
                       f"WHERE created > (NOW() - INTERVAL '1.25 hour');")
        result = cursor.fetchone()
    except conn.Error as error:
        print("Error connection to database", error)
    finally:
        if conn:
            conn.close()
    return result


if __name__ == '__main__':
    pass
