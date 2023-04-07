import datetime

from dotenv import load_dotenv, dotenv_values
from pymongo import MongoClient

load_dotenv()

HOST = dotenv_values('.env')['HOST']
PORT = int(dotenv_values('.env')['PORT'])
DB_NAME = dotenv_values('.env')['DB_NAME']
DB_COLLECTION = dotenv_values('.env')['DB_COLLECTION']

client = MongoClient(HOST, PORT)
db = client[DB_NAME]
collection = db[DB_COLLECTION]


def get_dataset_hour_group(items) -> dict:
    """
    Функция агрегации данных по часам
    """
    dataset = dict()

    for item in items:
        hour = item['dt'].strftime('%H')
        iso_format_time = f'T{hour}:00:00'
        date_for_dataset = item['dt'].date().isoformat() + iso_format_time
        if date_for_dataset in dataset.keys():
            dataset[date_for_dataset] += item['value']
        else:
            dataset[date_for_dataset] = item['value']

    dataset = dict(sorted(dataset.items()))
    return {"dataset": list(dataset.values()), "labels": list(dataset.keys())}


def get_dataset_day_group(items) -> dict:
    """
    Функция агрегации данных по дням
    """
    dataset = dict()

    for item in items:
        date_for_dataset = item['dt'].date().isoformat() + 'T00:00:00'
        if date_for_dataset in dataset.keys():
            dataset[date_for_dataset] += item['value']
        else:
            dataset[date_for_dataset] = item['value']

    dataset = dict(sorted(dataset.items()))
    return {"dataset": list(dataset.values()), "labels": list(dataset.keys())}


def get_dataset_month_group(items) -> dict:
    """
    Функция агрегации данных по месяцам
    """
    dataset = dict()

    for item in items:
        i_date = item['dt'].date()
        date_for_dataset = f"{i_date.strftime('%Y')}-{i_date.strftime('%m')}-01" + "T00:00:00"
        if date_for_dataset in dataset.keys():
            dataset[date_for_dataset] += item['value']
        else:
            dataset[date_for_dataset] = item['value']

    return {"dataset": list(dataset.values()), "labels": list(dataset.keys())}


def get_data(inp_data: dict) -> dict:
    """
    Получает данные из БД и вызывает соответствующую функцию
    """
    dt_from = datetime.datetime.fromisoformat(inp_data['dt_from'])
    dt_upto = datetime.datetime.fromisoformat(inp_data['dt_upto'])
    group_type = inp_data['group_type']

    items = collection.find({'dt': {'$gte': dt_from, '$lte': dt_upto}})

    if group_type == 'hour':
        dataset = get_dataset_hour_group(items)

    elif group_type == 'day':
        dataset = get_dataset_day_group(items)

    elif group_type == 'month':
        dataset = get_dataset_month_group(items)

    else:
        dataset = {}

    return dataset
