import re
import twitter_text

def parse_tweet(status, pos_emotes=[':\)', ':-\)', ': \)', ':D', \
        '=\)', '\(:', '\(-:', '\( :', '\(=', ':\]', ':-\]', '=\]', \
        '\[:', '\[-:', '\[=', ':o\)', ':3', ':c\)', '8\)', ':\}', ':^\)'], \
        neg_emotes=[':\(', '>:\[', ':-c', ':c', ':-<', ':<', ':\{', \
        '>.>', '<.<', '>.<', '>:\\\\', '>:/', ':-/', ':/', ':\\\\', '=/', \
        '=\\\\', ':S', ':-\(',': \(', '\):', '\)-:', '\) :']):
    """Removes urls, usernames, and repeated letters. Looks for emoticons
    and and finds tweet sentiment. Returns no message if it is a retweet,
    otherwise, returns the parsed tweet, along with a dictionary of 
    urls, usernames, and hashtags, with a sentiment code. Positive 
    sentiment = 1, negative sentiment = 0, no sentiment = None."""
    
    if 'RT' in status:
        return None
    ttext = twitter_text.TwitterText(status)
    extr = ttext.extractor
    
    urls =  extr.extract_urls_with_indices()
    hashtags = extr.extract_hashtags_with_indices()
    usernames = extr.extract_mentioned_screen_names_with_indices()
    (new_text, tweet_dic) = \
            replace_with_words(status, urls + hashtags + usernames)
    (new_text, positive, negative) = \
            remove_emoticons(new_text, pos_emotes, neg_emotes)

    if positive > 0 and negative > 0:
        return None
    elif positive > 0:
        sentiment = 1
    elif negative > 0:
        sentiment = -1
    else:
        sentiment = 0

    new_text = remove_multiple_repeats(new_text)
    
    return (new_text, tweet_dic, sentiment)

def replace_with_words(text, dic_list):
    """Function that replaces usernames with 'USER', hashtags with 
    'HASHTAG', and urls with 'URL'."""
    startend = []
    tweet_dic = {'URL':[], 'HASHTAG':[], 'USER':[]}
    for dic in dic_list:
        if 'url' in dic:
            value = dic['url']
            replace = 'URL'
        elif 'hashtag' in dic:
            value = dic['hashtag']
            replace = 'HASHTAG'
        elif 'screen_name' in dic:
            value = dic['screen_name']
            replace = 'USER'
        (start, end) = dic['indices']
        startend.append((start, end, replace, value))
    startend.sort()

    newstr = ''
    next_start = 0
    for i in xrange(len(startend)):
        tweet_dic[startend[i][2]].append(startend[i][3])
        newstr += text[next_start:startend[i][0]] + startend[i][2]
        next_start = startend[i][1]
    newstr += text[next_start:]
    return (newstr, tweet_dic)

def remove_multiple_repeats(text):
    repeat_list = []
    num_repeats = 0
    for i in xrange(1, len(text)):
        if text[i] == text[i-1]:
            num_repeats += 1
            if num_repeats > 1:
                repeat_list.append(i)
        else:
            num_repeats = 0
    indices = [text[i] for i in xrange(len(text)) if i not in repeat_list]
    return ''.join(indices)

def remove_emoticons(text, pos_emotes, neg_emotes):
    words = text.split()
    delete = []
    positive = 0
    for emote in pos_emotes:
        format_str = r'( ?%s | %s ?)' % (emote, emote)
        (text, count) = re.subn(format_str, ' ', text) 
        if count > 0: 
            positive += 1
    negative = 0
    for emote in neg_emotes:
        format_str = r'( ?%s | %s ?)' % (emote, emote)
        (text, count) = re.subn(format_str, ' ', text)
        if count > 0: 
            negative += 1
    return (text, positive, negative)


def reformat_word(word):
    """Returns a string which strips away non alpha-numeric characters and 
    turns everything to lowercase. For example, the function would 
    turn "Hello!" into "hello"."""
    word = word.lower()
    return ''.join([s for s in word if s.isalnum()])

if __name__ == '__main__':
    a = 'Rhema #Vaithianathan - Are Bad Managers To Blame For NZ\'s Poor Economic Performance? yhoo.it/ypM4yy : )  #via @scoopnz'
    print parse_tweet(a)
    b = 'hello :\ :/ my name is =/ =\ joooooohnnnmyname'
    print parse_tweet(b)

    a = '!!hello!?'
    print reformat_word(a)
