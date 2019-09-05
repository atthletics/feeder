from operators.s3 import S3ToDictList
from operators.mysql import DictListToMySQL
from argparse import ArgumentParser

def process(source, week_id, target_tbl):
    feed = S3ToDictList('atthletics', source, week_id)
    for data in feed.s3_data:
        DictListToMySQL(data, target_tbl)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-s", "--source")
    parser.add_argument("-w", "--week_id")
    parser.add_argument("-t", "--tbl")
    args = parser.parse_args()
    process(args.source, args.week_id, args.tbl)
