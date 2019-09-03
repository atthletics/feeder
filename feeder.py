import json
from datetime import datetime
import boto3
import MySQLdb

class S3ToStage():
    def __init__(self, bucket, date_param=None):
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.date_param = date_param
        if not date_param:
            self.date_param = datetime.today().strftime('%Y-%m-%d')

    def find_obj_keys(self, prefix):
        s3_objs = self.bucket.objects.filter(Prefix=prefix)
        obj_keys = [obj.key for obj in s3_objs]
        return(obj_keys)

    def get_data(self, obj_key):
        content_object = self.s3.Object(self.bucket.name, obj_key)
        content = content_object.get()['Body'].read()#.decode('utf-8')
        spreads_dict = json.loads(content)
        return(spreads_dict)
