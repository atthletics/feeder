import os, json
from datetime import datetime
import boto3
import MySQLdb

class S3ToStage():
    def __init__(self, bucket, source, week_id, date_param=None):
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        if not date_param:
            date_param = datetime.today().strftime('%Y-%m-%d')
        self.params = {
            'source'     : source,
            'week_id'    : week_id,
            'date_param' : date_param
        }
        prefix_path = 'data/{source}/week_id={week_id}/{date_param}'
        self.prefix = prefix_path.format(**self.params)
        self.s3_data = []

    def find_s3_objs(self):
        self.s3_objs = self.bucket.objects.filter(Prefix=self.prefix)
        self.s3_obj_keys = [obj.key for obj in self.s3_objs]
        return(self.s3_obj_keys)

    def get_s3_data(self, s3_obj_key):
        content_object = self.s3.Object(self.bucket.name, s3_obj_key)
        content = content_object.get()['Body'].read().decode('utf-8')
        data_dict = json.loads(content)
        scrape_ts = os.path.splitext(s3_obj_key.split('/')[3])[0]
        metadata = {
            'week_id' : self.params['week_id'],
            'scrape_ts' : scrape_ts
        }
        data_dict = [game.update(metadata) for game in data_dict]
        return(data_dict)

    def main(self):
        self.find_s3_objs()
        for s3_obj_key in self.s3_obj_keys:
            self.s3_data.append(self.get_s3_data(s3_obj_key))
