import twitter
import sqlite3
import Queue
import unicodedata

class Controller(object):
    """Object for controlling and obtaining data from the twitter streams. 
    Takes this data then puts it into a SQL database."""
    def __init__(self, track_word_list, dbname="twitterdb"):
        self.track_word_list = track_word_list
        self.api = twitter.Api()
        self.db = sqlite3.connect(dbname)
        self.cursor = self.db.cursor()
        self.create_sql_database()

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
                new_additions += self.db_insert_search(search)
                print word, i, len(search)
            except twitter.TwitterError:
                NoError = False
            i += 1
        return new_additions

    def create_sql_database(self):
        try:
            string = ('create table twitterdb(status_id int'
                'primary key, datetime text, ' 
                'msg_text text, location text, user_id text, '
                'user_screen_name text, '
                'user_location text, user_followers_count int, '
                'user_statuses_count int, user_friends_count int)')
            self.cursor.execute(string)
            self.db.commit()
        except sqlite3.OperationalError:
            pass

    def db_insert_search(self, search):
        new_additions = 0 
        unfinished = Queue.Queue()
        for s in search:
            unfinished.put(s)
        while not unfinished.empty():
            status = unfinished.get()
            user = status.user
            data_tuple = (status.id, status.created_at, 
                    status.text, status.location, user.id, user.screen_name,
                    user.location, user.followers_count,
                    user.statuses_count, user.friends_count)
            try:
                self.cursor.execute('''insert into twitterdb values 
                        (?,?,?,?,?,?,?,?,?,?)''', \
                        data_tuple)
                unfinished.task_done()
                self.db.commit()
                new_additions += 1
            except sqlite3.OperationalError:
                unfinished.put(status)
                unfinished.task_done()
            except sqlite3.IntegrityError:
                pass
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

    def select_duplicates(self, category="status_id"):
        execute_cmd = ('select status_id, count(status_id) '
                'as numoccurrences from twitterdb group by '
                'status_id having (count(status_id) > 1)')
        return self.db.execute(execute_cmd):


def test():
    api = twitter.Api()
    statuses = api.GetPublicTimeline()
    print [s.created_at for s in statuses]
    print [s.id for s in statuses]
    print [s.text for s in statuses]
    print [s.location for s in statuses]
    print [s.user.screen_name for s in statuses]
    print [s.user.location for s in statuses]
    print [s.user.followers_count for s in statuses]
    print [s.hashtags for s in statuses]

if __name__ == '__main__':
    #test()
   
    a = AnalyzeDatabase()
    a.select_duplicates()
    word_list = ['economy', 'jobs', 'finance', 'recession', 'stock market']
    control = Controller(word_list)
    control.get_all_wordlist_statuses()

