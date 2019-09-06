from operators.s3 import S3ToDictList
from operators.mysql import DictListToMySQL
from argparse import ArgumentParser

def process(week_id, date_id):
    os_feed = S3ToDictList('atthletics', 'os', week_id, date_id)
    for data in os_feed.s3_data:
        DictListToMySQL(data, 'ncaaf_os_scrape')

    es_feed = S3ToDictList('atthletics', 'es', week_id, date_id)
    for data in es_feed.s3_data:
        DictListToMySQL(data, 'ncaaf_es_scrape')

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-w", "--week_id",
                        help = "Required Week ID to pull data from S3")
    parser.add_argument("-d", "--date_id",
                        help = "Optional Date ID for historic processing",
                        default = None)
    args = parser.parse_args()
    process(args.week_id, args.date_id)
