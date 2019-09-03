import json
from datetime import datetime
import boto3
import MySQLdb

class LoadSpreads():
    def __init__(self, bucket, date_param=None):
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.date_param = date_param
        if not date_param:
            self.date_param = datetime.today().strftime('%Y-%m-%d')

    def find_objs(self, prefix):
        s3_objs = self.bucket.objects.filter(Prefix=prefix)
        obj_keys = [obj.key for obj in s3_objs]
        return(obj_keys)

    def get_spreads(self, obj_key):
        content_object = self.s3.Object(self.bucket.name, obj_key)
        content = content_object.get()['Body'].read()#.decode('utf-8')
        spreads_dict = json.loads(content)
        return(spreads_dict)

    def main(self):
        espn_prefix = 'espn/spreads/{0}'.format(self.date_param)
        espn_obj_key = self.find_objs(espn_prefix)
        espn_spreads = self.get_spreads(espn_obj_key)

        os_prefix = 'oddsshark/spreads/{0}'.format(self.date_param)
        os_obj_key = self.find_objs(os_prefix)
        os_spreads = self.get_spreads(os_obj_key)
        print(espn_spreads, os_spreads)

if __name__ == '__main__':
    LoadSpreads('atthletics').main()
