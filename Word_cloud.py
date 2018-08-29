#   with open('C:/Users/Joseph.DI-TROLIO/Desktop/files/RC_{}'.format(timeframe, timeframe), buffering=1000) as f:
#   http://files.pushshift.io/reddit/comments/

import sqlite3
import json
from datetime import datetime


timeframe = '2018-06'
sql_transaction = []
start_row = 0
cleanup = 5000000

# keyword_counterNew = 0
# keyword_counterOld = 0

connection = sqlite3.connect('{}.db'.format('Word_cloud'))
c = connection.cursor()

def create_table():
    c.execute(
        #Name of table - word_cloud
        "CREATE TABLE IF NOT EXISTS word_cloud (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, "
        "comment TEXT, subreddit TEXT, unix INT, score INT, keyword_score INT)")


def format_data(data):
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []

def sql_insert(commentid, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO word_cloud (comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}");""".format(
            commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def acceptable(data):
    if data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True

if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0

    # with open('D:/Téléchargements/{}/RC_{}'.format(timeframe.split('-')[0],timeframe), buffering=1000) as f:
    with open('D:/Final project/RC_{}'.format(timeframe, timeframe), buffering=1000) as f:
        for row in f:
            # print(row)
            # time.sleep(555)
            row_counter += 1

            if row_counter > start_row:
                try:
                    row = json.loads(row)
                    body = format_data(row['body'])
                    created_utc = row['created_utc']
                    score = row['score']

                    parent_id = row['parent_id'].split('_')[1]

                    comment_id = row['id']

                    subreddit = row['subreddit']

                    if acceptable(body) and subreddit == "Advice":
                        sql_insert(comment_id, body, subreddit, created_utc, score)

                except Exception as e:
                    print(str(e))

            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Time: {}'.format(row_counter, str(datetime.now())))

