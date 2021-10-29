from pymongo import MongoClient
import pymongo
from datetime import datetime, timedelta
import os
import random


login = os.getenv('LOGIN_M')
password = os.getenv('PASSWORD_M')

client = MongoClient(f'mongodb://{login}:{password}@localhost:27017/')
db = client['temp_krasnodar']
collection = db[f'{datetime.now().strftime("%b_%Y")}']


def select_by_hour():
    """ Выборка за последний час """
    result = []
    end = datetime.now()
    start = end - timedelta(days=1)
    query = {"created": {"$gte": start, "$lte": end}}
    for i in collection.find(query,
                             {"_id": 0, "temperature": 1, "time": 1, "view_date": 1, "street": 1}) \
            .sort("created", pymongo.DESCENDING):
        result.append(i)
    return result


def select_last_data() -> dict:
    """ Дата из крайней записи """
    end = datetime.now()
    start = end - timedelta(hours=0.25)
    query = {"created": {"$gte": start, "$lte": end}}
    result = collection.find_one(query,
                             {"_id": 0, "created": 1})
    return result


def select_by_hour_one_random_street():
    """ 10 полей по случайной улице (улица была за последний час) """
    result = []
    filt = {"street": random.choice(select_by_hour())['street']}
    query = collection.find(filt, {"_id": 0, "temperature": 1, "time": 1, "view_date": 1, "street": 1}
        ).sort('created', -1).limit(10)
    for i in query:
        result.append(i)
    return result


def select_avg_hour():
    """ Средняя температура за час """
    end = datetime.now()
    start = end - timedelta(hours=1.25)
    agg_result = collection.aggregate([
        {"$match": {"created": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": 0, "AVG": {"$avg": "$temperature"}}}
    ])
    for i in agg_result:
        return round(i['AVG'], 2)


def max_min_temp_last_query():
    """ Выборка за день, начиная с 00:00
    сортировка по температуре
    Первая - Максимальная
    Последняя - Минимальная """
    result = []
    end = datetime.now()
    delta = datetime.now().strftime("%H %M %S").split()
    start = end - timedelta(hours=int(delta[0]), minutes=int(delta[1]), seconds=int(delta[2]))
    query = {"created": {"$gte": start, "$lte": end}}
    for i in collection.find(query,
                             {"_id": 0, "temperature": 1, "time": 1, "view_date": 1, "street": 1}) \
            .sort("time", pymongo.DESCENDING):
        result.append(i)
    return result


if __name__ == '__main__':
    pass
