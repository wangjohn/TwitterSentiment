import twitter
import sqlite3
import Queue
import unicodedata
import time
import datetime
import csv

class Controller(object):
    """Object for controlling and obtaining data from the twitter streams. 
    Takes this data then puts it into a SQL database."""
    def __init__(self, track_word_list, dbname="twitterdb"):
        self.track_word_list = track_word_list
        self.api = twitter.Api()
        self.db = sqlite3.connect(dbname)
        self.cursor = self.db.cursor()
        self.create_sql_database()

    def continue_grabbing(self, minutes=10):
        while True:
            self.get_all_wordlist_statuses()
            time.sleep(minutes*60)

    def get_all_wordlist_statuses(self):
        additions = []
        for i in xrange(len(self.track_word_list)):
            word = self.track_word_list[i]
            additions.append(self.get_statuses(word))
        new_additions = [(additions[i], self.track_word_list[i]) for i in \
                xrange(len(self.track_word_list))]
        print new_additions

    def get_statuses(self, word):
        NoError = True
        i = 1
        count = 0
        new_additions = 0
        while NoError:
            try:
                search = self.api.GetSearch(word, per_page=100, page=i)
                count += len(search)
                new_additions += self.db_insert_search(search, word)
                print word, i, len(search)
            except twitter.TwitterError:
                NoError = False
            i += 1
        return new_additions

    def create_sql_database(self):
        try:
            string = ('create table twitterdb(status_id int'
                'primary key, keyword text, datetime timestamp, ' 
                'msg_text text, location text, user_id text, '
                'user_screen_name text, '
                'user_location text, user_followers_count int, '
                'user_statuses_count int, user_friends_count int)')
            self.cursor.execute(string)
            self.db.commit()
        except sqlite3.OperationalError:
            pass

    def db_insert_search(self, search, keyword):
        new_additions = 0 
        unfinished = Queue.Queue()
        for s in search:
            unfinished.put(s)
        while not unfinished.empty():
            status = unfinished.get()
            user = status.user
            dtobj = datetime.datetime.strptime(\
                    status.created_at[:-5], '%a, %d %b %Y %H:%M:%S ') 
            time = dtobj.strftime('%Y-%m-%d %H:%M:%S')
            data_tuple = (status.id, keyword, time,
                    status.text, status.location, user.id, user.screen_name,
                    user.location, user.followers_count,
                    user.statuses_count, user.friends_count)
            try:
                self.cursor.execute('''insert into twitterdb values 
                        (?,?,?,?,?,?,?,?,?,?,?)''', \
                        data_tuple)
                self.db.commit()
                new_additions += 1
            except sqlite3.OperationalError:
                unfinished.put(status)
            except sqlite3.IntegrityError:
                print 'Fail'
                pass
            unfinished.task_done() 
        return new_additions


class AnalyzeDatabase(object):
    """Class for analyzing and performing queries on the database."""
    def __init__(self, dbname="twitterdb"):
        self.db = sqlite3.connect(dbname)
        self.cursor = self.db.cursor()
    
    def select_text(self):
        text_list = []
        for row in self.db.execute(\
                "select msg_text, status_id from twitterdb"):
            words = unicodedata.normalize('NFKD', row[0]).encode('ascii',\
                    'ignore')
            text_list.append(words)
        return text_list

    def select_dates(self):
        date_list = []
        for row in self.db.execute(('select datetime from twitterdb')):
            dtobj = datetime.datetime.strptime(row[0], \
                    '%Y-%m-%d %H:%M:%S')
            date_list.append(dtobj)
        date_list.sort(key = lambda d: (d.year, d.month, d.day, \
                d.hour, d.minute))
        return date_list

    def get_day_counts(self):
        dates = {}
        date_list = self.select_dates()
        for d in date_list:
            date = (d.year, d.month, d.day) 
            try:
                dates[date] += 1
            except KeyError:
                dates[date] = 1
        return dates

    def get_hour_counts(self):
        hours = {}
        date_list = self.select_dates()
        for d in date_list:
            try:
                hours[d.hour] += 1
            except KeyError:
                hours[d.hour] = 1
        return hours

    def get_keyword_counts(self):
        keywords = {}
        for row in self.db.execute('select keyword from twitterdb'):
            kw = unicodedata.normalize('NFKD', row[0]).encode(\
                    'ascii', 'ignore')
            try:
                keywords[kw] += 1
            except KeyError:
                keywords[kw] = 1
        return keywords

    def get_keyword_day_counts(self):
        kwday = {}
        for row in self.db.execute(\
                'select keyword, datetime from twitterdb'):
            keyword = unicodedata.normalize('NFKD', row[0]).encode(\
                    'ascii', 'ignore')
            dtobj = datetime.datetime.strptime(row[1], \
                    '%Y-%m-%d %H:%M:%S')
            date = (dtobj.year, dtobj.month, dtobj.day) 
            try:
                kwday[(date, keyword)] += 1
            except KeyError:
                kwday[(date, keyword)] = 1
        return kwday

    def make_csv(self, filename, start_date, end_date):
        """start_date and end_date should be datetime objects while 
        filename should be a string such as 'this_file.csv' for which
        you would like to write the data."""

        start = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end = end_date.strftime('%Y-%m-%d %H:%M:%S')
        cmd = ('select * from twitterdb where ' \
                'datetime between \"%s\" and \"%s\"') \
                % (start_date, end_date)
        writer = csv.writer(open(filename, 'wb'))
        for row in self.db.execute(cmd):
            new_row = []
            for s in row:
                if isinstance(s, unicode): 
                    new_row.append(s.encode('utf-8'))
                else:
                    new_row.append(s)
            writer.writerow(new_row)


def parse_tweet(status, pos_emotes=[':)', ':-)', ': )', ':D', '=)'], \
        neg_emotes=[':(',':-(',': (']):
    """Removes urls, usernames, and repeated letters. 
    Also looks for emoticons."""
    text = status.text
    if status.urls != None:
        for url in status.urls:
            split_text = text.split(url)
            text = 'URL'.join(split_text)
    
    if status.user_mentions != None:
        for user in status.user_mentions:
            split_text = text.split(user)
            text = 'USERNAME'.join(split_text)
    
    delete = []
    for i in xrange(len(text)):
        prev1 = None
        prev2 = None
        if prev2 == prev1 and prev1 == text[i]:
            delete.append(i)
        prev2 = prev1
        prev1 = text[i]
    new_indices = [i for i in xrange(text) not in delete]
    text = ''.join([text[i] for i in new_indices])

    sentiment = 0
    both = False
    for emote in pos_emotes:
        if emote in text:
            sentiment += 1
            text = ''.join(text.split(emote))
    for emote in neg_emotes:
        if emote in text:
            if (sentiment > 0) and (both == False):
                both = True
            sentiment -= 1
            text = ''.join(text.split(emote))
    return (text, sentiment, both)


def test():
    api = twitter.Api()
    while True:
        statuses = api.GetPublicTimeline()
        a = [s.user_mentions for s in statuses if s.user_mentions is not None]
        b = [s.urls for s in statuses if s.urls is not None]
        print a, b

if __name__ == '__main__':
    #test()
    a = AnalyzeDatabase()
    print a.get_day_counts()
    print a.get_hour_counts()
    print a.get_keyword_day_counts()
    start = datetime.datetime(2012, 1, 1, 0, 0)
    end = datetime.datetime(2012, 3, 1, 0, 0)
    a.make_csv('testpull.csv', start, end)
    word_list = ['economy', 'jobs', 'finance', 'recession', 'stock market']
    control = Controller(word_list)
    control.get_all_wordlist_statuses()

