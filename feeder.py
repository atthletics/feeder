from operators.s3 import S3ToDictList
from operators.mysql import DictListToMySQL
from argparse import ArgumentParser

def process(week_id):
    os_feed = S3ToDictList('atthletics', 'os', week_id)
    for data in os_feed.s3_data:
        DictListToMySQL(data, 'ncaaf_os_scrape')

    es_feed = S3ToDictList('atthletics', 'es', week_id)
    for data in es_feed.s3_data:
        DictListToMySQL(data, 'ncaaf_es_scrape')

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-w", "--week_id")
    args = parser.parse_args()
    process(args.week_id)
