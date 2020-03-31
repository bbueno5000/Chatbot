"""
DOCSTRING
"""
import json
import sqlite3

TIMEFRAME = '2015-05'
SQL_TRANSACTION = []
CONNECTION = sqlite3.connect('{}2.db'.format(TIMEFRAME))
c = CONNECTION.cursor()

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

if __name__ == '__main__':
    create_table()
    ROW_COUNTER = 0
    PAIRED_ROWS = 0
    with open('RC_2015-01', buffering=1000) as file:
        for row in file:
            ROW_COUNTER += 1
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
