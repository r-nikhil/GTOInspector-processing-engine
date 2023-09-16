import os
import json
import datetime
import pymongo
from . import exceptions


basedir = os.path.dirname(os.path.abspath(__file__))

parentdir = os.path.dirname(basedir)


def get_config():
    with open(os.path.join(parentdir, "config.json"), "r") as f:
        return json.loads(f.read())


configdict = get_config()

path_to_pem_file = os.path.join(parentdir, "hidden",
                                "ec2-hosting.pem")


class BaseDB(object):

    def __init__(self) -> None:
        self.dbconn = self.create_db()

    def create_db(self):
        conn_string = "mongodb://{}:{}@{}:{}".format(configdict.get("MONGO_USER"), configdict.get(
            "MONGO_PWD"), configdict.get("MONGO_HOST"), configdict.get("MONGO_PORT"))
        try:
            mongo = pymongo.MongoClient(conn_string)[configdict.get(
                "MONGO_PROCESSING_QUEUE")]["file_list"]
            print("Connected to database: {}".format(conn_string))
        except Exception as e:
            print(e)
            print("Could not connect")
        return mongo

    def add_file(self, filename: str, upload_time: datetime.datetime, email: str, format: str):
        self.dbconn.insert_one(dict(filename=filename, upload_time=upload_time, email=email, num_hands=0,
                                    num_hands_processed=0,  format=format, is_processed=False, processing_time=0))
        return True

    def get_files(self):
        res = self.dbconn.find({"is_processed": False})
        return res

    def close_connection(self):
        self.dbconn.close()

    def update_file_metadata(self, filename, **kwargs):
        data = self.dbconn.find_one({"filename": filename})
        if data is not None:
            is_processed = kwargs.get("is_processed", False)
            num_hands = kwargs.get("num_hands", 0)
            num_hands_processed = kwargs.get("num_hands_processed", 0)
            processing_time = kwargs.get("processing_time", 0)
            mail_sent = kwargs.get("mail_sent", False)
            self.dbconn.update({"filename": filename}, {"$set": dict(
                is_processed=is_processed,
                num_hands=num_hands,
                num_hands_processed=num_hands_processed,
                processing_time=processing_time,
                mail_sent=mail_sent
            )})
            return True
        else:
            raise exceptions.FileEntryNotFoundError("File entry not found")

    def get_list_of_files(self):
        data = self.dbconn.find(limit=500, projection={
                                "filename": True, "_id": False})
        return data


if __name__ == '__main__':
    pass
