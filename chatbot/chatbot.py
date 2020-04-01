"""
DOCSTRING
"""
# standard
import datetime
# non-standard
import json
import pandas
import sqlite3

timeframes = '2015-05'
sql_transaction = []
for timeframe in timeframes:
    connection = sqlite3.connect('{}.db'.format(timeframe))
    c = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False
    while cur_length == limit:
        df = pd.read_sql(
            "SELECT * FROM parent_reply WHERE unix > {} and \
            parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,
                                                                             limit), connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        if not test_done:
            with open('test.from','a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open('test.to','a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(str(content)+'\n')
            test_done = True
        else:
            with open('train.from','a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open('train.to','a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(str(content)+'\n')
        counter += 1
        if counter % 20 == 0:
            print(counter*limit,'rows completed so far')

def acceptable(data):
    """
    DOCSTRING
    """
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True

def create_table():
    """
    DOCSTRING
    """
    c.execute(
        "CREATE TABLE IF NOT EXISTS parent_reply( \
        parent_id TEXT PRIMARY KEY, \
        comment_id TEXT UNIQUE, \
        parent TEXT, \
        comment TEXT, \
        subreddit TEXT, \
        unix INT, \
        score INT)")

def find_existing_score(pid):
    """
    DOCSTRING
    """
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        #print(str(e))
        return False

def find_parent(pid):
    """
    DOCSTRING
    """
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result is not None:
            return result[0]
        else: 
            return False
    except Exception as exception:
        return False

def format_data(data):
    """
    DOCSTRING
    """
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    """
    DOCSTRING
    """
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))


def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    """
    DOCSTRING
    """
    try:
        sql = """INSERT INTO parent_reply (parent_id, \
        comment_id, comment, subreddit, unix, score) \
        VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))

def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, time, score):
    """
    DOCSTRING
    """
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, \
        comment_id = ?, \
        parent = ?, \
        comment = ?, \
        subreddit = ?, \
        unix = ?, \
        score = ? \
        WHERE parent_id =?;""".format(parentid,
                                      commentid,
                                      parent,
                                      comment,
                                      subreddit,
                                      int(time),
                                      score,
                                      parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))

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

if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0
    with open('RC_2015-01', buffering=1000) as file:
        for row in file:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)
            if score >= 2:
                existing_comment_score = find_existing_score(parent_id)
                if existing_comment_score:
                    if score > existing_comment_score:
                         if acceptable(body):
                            sql_insert_replace_comment(comment_id,
                                                       parent_id,
                                                       parent_data,
                                                       body,
                                                       subreddit,
                                                       created_utc,
                                                       score)
                else:
                    if acceptable(body):
                        if parent_data:
                            sql_insert_has_parent(comment_id,
                                                  parent_id,
                                                  parent_data,body,
                                                  subreddit,
                                                  created_utc,
                                                  score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(comment_id,
                                                 parent_id,
                                                 body,
                                                 subreddit,
                                                 created_utc,
                                                 score)
            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter,
                                                                              paired_rows,
                                                                              str(datetime.now())))