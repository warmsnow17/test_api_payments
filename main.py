import bson
import pymongo
from fastapi import FastAPI
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

app = FastAPI()


def add_missing_dates(labels, dataset, dt_from, dt_upto, date_format, delta):
    current_date = dt_from
    new_labels = []
    new_dataset = []
    index = 0

    while current_date <= dt_upto:
        current_date_str = current_date.strftime(date_format)

        if index < len(labels) and labels[index] == current_date_str:
            new_labels.append(labels[index])
            new_dataset.append(dataset[index])
            index += 1
        else:
            new_labels.append(current_date_str)
            new_dataset.append(0)

        current_date += delta

    return new_labels, new_dataset


@app.get("/data/")
async def get_data(dt_from: str, dt_upto: str, group_type: str):

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mydatabase"]
    collection = db["sample_collection"]

    collection.drop()

    with open("sample_collection.bson", "rb") as f:
        data = f.read()
        for doc in bson.decode_all(data):
            collection.insert_one(doc)

    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    date_format_dict = {
        "month": {"format": "%Y-%m-01T00:00:00", "delta": relativedelta(months=1)},
        "day": {"format": "%Y-%m-%dT00:00:00", "delta": timedelta(days=1)},
        "hour": {"format": "%Y-%m-%dT%H:00:00", "delta": timedelta(hours=1)},
    }

    if group_type not in date_format_dict:
        return {"error": "Invalid group_type"}

    date_format = date_format_dict[group_type]["format"]
    delta = date_format_dict[group_type]["delta"]

    pipeline = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {"_id": {"$dateToString": {"format": date_format, "date": "$dt"}}, "sum_value": {"$sum": "$value"}}},
        {"$sort": {"_id": 1}}
    ]

    result = list(collection.aggregate(pipeline))
    labels = [i["_id"] for i in result]
    dataset = [i["sum_value"] for i in result]

    labels, dataset = add_missing_dates(labels, dataset, dt_from, dt_upto, date_format, delta)

    return {"dataset": dataset, "labels": labels}
