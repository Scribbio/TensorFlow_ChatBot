#   with open('C:/Users/Joseph.DI-TROLIO/Desktop/files/RC_{}'.format(timeframe, timeframe), buffering=1000) as f:
#   http://files.pushshift.io/reddit/comments/

import sqlite3
import json
from datetime import datetime
import time

timeframe = '2018-06'
sql_transaction = []
start_row = 0
cleanup = 5000000

# keyword_counterNew = 0
# keyword_counterOld = 0

connection = sqlite3.connect('{}.db'.format('2018-06'))
c = connection.cursor()

#
# 'how’s everything?', 'how are things?', 'how’s life?', 'how’s your day?',         'how’s your day going?',
# 'good to see you', 'nice to see you', 'long time no see', 'it’s been a while',            'good morning',
# 'good afternoon', 'good evening', 'it’s nice to meet you', 'pleased to meet you',
#             'welcome', 'how have you been?', 'yo!', 'are you OK?', 'you alright?', 'alright mate?', 'howdy!', 'sup?',
#             'whazzup?', 'whassup', 'what’s up', 'hiya!', ,

# keywords = ['how are you?', 'how’s it going?', 'how are you doing?', 'what’s up?', 'what’s new?'
#             'what’s going on?',
#             ' atmosphere', ' clear sky', ' climate', ' cold', ' cyclone', ' fog',
#             ' humidity', ' humid', ' precipitation', ' prevailing wind', ' rain ', ' rainfall', ' summer', ' winter', ' autumn', 'spring', 'seasonality',
#             'smog ', ' sunny', ' snow', ' snowflakes', ' snowflakes', ' snowstorm ', ' temperature', ' temperature range', ' warm front', ' warm sector',
#             ' warmer', ' weather', ' wind ', ' windy ', 'drizzle', 'downpour', 'freezing', 'cloudy', 'overcast', 'hurricane', ' drought', 'thunder', 'met office',
#             'meteorology', 'rainstorm', 'cloudless', 'tsunami', 'tornado']

keywords = ['people', 'friend', 'time', 'something', 'work', 'help', 'feel', 'life', 'tell', 'now', 'someone', 'take',
            'good', 'sure', 'much', 'person', 'look', 'sound', 'probably', 'job', 'right', 'anything', 'parent', 'never',
            'give', 'relationship', 'always', 'ask', 'said', 'thanks', 'talk', 'better', 'find', 'actually', 'best',
            'love', 'problem', 'situation', 'girl', 'feeling', 'family', 'point', 'kid', 'start', 'trying', 'live',
            'year', 'thought', 'kind', 'around', 'place', 'hard', 'little', 'end', 'understand', 'school', 'bad',
            'reason', 'everyone', 'issue', 'money', 'everything', 'already', 'nothing', 'change', 'leave', 'stop',
            'sometime', 'mom', 'able', 'experience', 'care', 'believe', 'enough', 'advice', 'anyone', 'happy', 'sorry',
            'other', 'Good luck', 'mind', 'another', 'matter', 'college', 'two', 'great', 'without',  'saying', 'happen',
            'call', 'part', 'told', 'seems', 'home' 'us', 'around']


def create_table():
    c.execute(
        #Name of table - parent reply
        "CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, "
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


def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, time, score, keywordsCount):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ?, keyword_score = ?, WHERE parent_id =?;""".format(
            parentid, commentid, parent, comment, subreddit, int(time), score, keywordsCount, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score, keywordsCount):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score, keyword_score) VALUES ("{}","{}","{}","{}","{}","{}","{}","{}");""".format(
            parentid, commentid, parent, comment, subreddit, int(time), score, keywordsCount)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score, keywordsCount):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score, keyword_score) VALUES ("{}","{}","{}","{}","{}","{}","{}");""".format(
            parentid, commentid, comment, subreddit, int(time), score, keywordsCount)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def acceptable(data):
    if len(data.split(' ')) > 1000 or len(data) < 1:
        return False
    elif len(data) > 32000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True

#def clean(txt):
   # prohibitedWords = [" newlinechar ", "&gt;", "&amp;"]
   # result = txt
   # for word in prohibitedWords:
   #     result = result.replace(word, "")
   # return result

def keywords_count(txt):
    counter = 0
    for w in keywords:
        if w.lower() in txt:
            counter = counter + 1
    return counter


def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))
        return False


def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))
        return False

def find_existing_keyword_score(pid):
    try:
        sql = "SELECT keyword_score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))
        return False


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
                    parent_id = row['parent_id'].split('_')[1]
                    body = format_data(row['body'])
                    created_utc = row['created_utc']
                    score = row['score']

                    comment_id = row['id']

                    subreddit = row['subreddit']


                    keywordsCount = keywords_count(body)

                    parent_data = find_parent(parent_id)

                    #This first block looks to see if an existing comment exist, and replaces it if the current row has a higher score

                    if keywordsCount > 0:
                        existing_comment_score = find_existing_score(parent_id)
                        existing_keyword_score = find_existing_keyword_score(parent_id)
                        if existing_comment_score: #false if no existing comment exists
                            if score > existing_comment_score or keywordsCount > existing_keyword_score:
                                if acceptable(body):
                                    sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit,
                                                               created_utc, score, keywordsCount)

                        #If no particular existing comment exists, we insert new comment
                        else:
                            if acceptable(body):
                                if parent_data: #if comment has a parent that it is replying to
                                    #if score >= 2: #This is the comment score threshold - designed to filter only good comments - won't be used as we need to maximise our comment number
                                        sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit,
                                                              created_utc, score, keywordsCount)
                                        paired_rows += 1
                                else: #comment is inserted with no parent
                                    sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score, keywordsCount)
                except Exception as e:
                    print(str(e))

            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows,
                                                                              str(datetime.now())))

            if row_counter % cleanup == 0:
                    print("Cleanin up!")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    c.execute(sql)
                    connection.commit()
                    c.execute("VACUUM")
                    connection.commit()
