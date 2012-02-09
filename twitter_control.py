import twitter
import sqlite3
import Queue
import unicodedata
import time
import datetime
import csv
import threading
import signal
import sys



class TimeoutException(Exception):
    pass

class TimeoutFunction(object):
    """Object for making a TimeoutException if a function has run 
    on for too long without a response."""
    def __init__(self, function, timeout):
        self.timeout = timeout
        self.function = function

    def handle_timeout(self, signum, frame):
        raise TimeoutException()

    def __call__(self, word, per_page, page):
        old = signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.timeout)
        try:
            result = self.function(word, per_page=per_page, page=page)
        finally:
            signal.signal(signal.SIGALRM, old)
        signal.alarm(0)
        return result

class Controller(object):
    """Object for controlling and obtaining data from the twitter streams. 
    Takes this data then puts it into a SQL database."""
    def __init__(self, track_word_list, dbname="twitterdb", timeout=20):
        self.track_word_list = track_word_list
        self.api = twitter.Api()
        self.db = sqlite3.connect(dbname)
        self.cursor = self.db.cursor()
        self.create_sql_database()
        self.timeout = timeout

    def continue_grabbing(self, minutes=10, multithread=False):
        while True:
            if multithread:
                self.get_all_statuses_multithread()
            else:
                self.get_all_statuses()
            time.sleep(minutes*60)

    def get_all_statuses_multithread(self, threads=3):
        additions = 0
        word_queue = Queue.Queue()
        queue = Queue.Queue()
        for word in self.track_word_list:
            word_queue.put(word)
        for i in xrange(threads):
            gs = GetStatuses(word_queue, queue, self.timeout)
            gs.start()
        for i in xrange(1):
            dbinsert = DatabaseInsert(queue, additions)
            dbinsert.setDaemon(True)
            dbinsert.start()
        word_queue.join()
        queue.join()
        print additions

    def get_all_statuses(self):
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
                searchFun = TimeoutFunction(self.api.GetSearch, self.timeout)
                search = searchFun(word, 100, i)
                count += len(search)
                new_additions += self.db_insert_search(search, word)
                print word, i, len(search)
            except twitter.TwitterError:
                NoError = False
            except TimeoutException:
                i -= 1
                print 'Timeout Exception: %s, %s' % (word, str(i))
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

class DatabaseInsert(threading.Thread):
    def __init__(self, queue, new_additions, dbname='twitterdb',
            max_attempts = 1000):
        threading.Thread.__init__(self)
        self.queue = queue
        self.new_additions = new_additions
        self.db = sqlite3.connect(dbname, check_same_thread=False)
        self.cursor = self.db.cursor()
        self.max_attempts = max_attempts

    def run(self):
        while (not self.queue.empty()):
            (status, keyword, attempts) = self.queue.get()
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
                self.new_additions += 1
                print status.id
            except sqlite3.OperationalError:
                if attempts < self.max_attempts:
                    self.queue.put((status, keyword, attempts+1))
            except sqlite3.IntegrityError:
                pass
            self.queue.task_done() 

class GetStatuses(threading.Thread):
    def __init__(self, word_queue, queue, timeout=20):
        threading.Thread.__init__(self)
        self.word_queue = word_queue
        self.queue = queue
        self.timeout = timeout
        self.api = twitter.Api()

    def run(self):
        while (not self.word_queue.empty()):
            word = self.word_queue.get()
            NoError = True
            i = 1
            while NoError:
                try:
                    searchFun = TimeoutFunction(self.api.GetSearch, \
                            self.timeout)
                    search = searchFun(word, 100, i)
                    for s in search:
                        self.queue.put((s, word, 0))
                    print word, i, len(search)
                except twitter.TwitterError:
                    NoError = False
                except TimeoutException:
                    i -= 1
                    print 'Timeout Exception: %s, %s' % (word, str(i))
                i += 1
            self.word_queue.task_done()


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
        writer.writerow(['Status ID', 'Keyword', 'Time',\
                'Tweet', 'Location', 'User ID', 'User Screen Name', \
                'User Location', 'User Followers Count', \
                'User Statuses Count', 'User Friends Count'])
        for row in self.db.execute(cmd):
            new_row = []
            for s in row:
                if isinstance(s, unicode): 
                    new_row.append(s.encode('utf-8'))
                elif s == None:
                    new_row.append(' ')
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
    if 'RT' in text:
        retweet = True
    else:
        retweet = False
    return (text, sentiment, both, retweet)


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
    start = datetime.datetime(2012, 2, 1, 0, 0)
    end = datetime.datetime(2012, 2, 12, 0, 0)
    a.make_csv('testpull.csv', start, end)
    word_list = ['economy', 'jobs', 'finance', 'recession', 'stock market']
    control = Controller(word_list)
    #control.get_all_statuses_multithread()
    control.get_all_statuses()

