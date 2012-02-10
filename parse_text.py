import re
import twitter_text

def parse_tweet(status, pos_emotes=[':)', ':-)', ': )', ':D', '=)'], \
        neg_emotes=[':(',':-(',': (']):
    """Removes urls, usernames, and repeated letters. Looks for emoticons
    and also returns whether it is a retweet."""
    text = status.text

    word_split_re = re.compile(r'(\s+)')
    words = word_split_re.split(text)
    url = check_for_url(text)
    if 'RT' in words:
        return None

def parse_tweet2(status):
    if 'RT' in status:
        return None
    ttext = twitter_text.TwitterText(status)
    extr = ttext.extractor
    
    urls =  extr.extract_urls_with_indices()
    hashtags = extr.extract_hashtags_with_indices()
    usernames = extr.extract_mentioned_screen_names_with_indices()
    new_text = replace_with_words(status, urls + hashtags + usernames)
    
    # Check for emoticons

    new_text = remove_multiple_repeats(new_text)
    
    
    return new_text

def replace_with_words(text, dic_list):
    """Function that replaces usernames with 'USER', hashtags with 
    'HASHTAG', and urls with 'URL'."""
    startend = []
    for dic in dic_list:
        if 'url' in dic:
            replace = 'URL'
        elif 'hashtag' in dic:
            replace = 'HASHTAG'
        elif 'screen_name' in dic:
            replace = 'USER'
        (start, end) = dic['indices']
        startend.append((start, end, replace))
    startend.sort()
    newstr = text[:startend[0][0]] + startend[0][2]
    next_start = startend[0][1]
    for i in xrange(1, len(startend)):
        newstr += text[next_start:startend[i][0]] + startend[i][2]
        next_start = startend[i][1]
    newstr += text[next_start:]
    return newstr

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
    
def check_for_url(text):
    # Check for different urls that can be on twitter 
    words = text.split()
    simple_url_re = re.compile(r' (https?://.*?) ')
    simple_url_2_re = re.compile(r'www\.|^(?!http)\w[^@]+\.(com|edu|gov|int|mil|net|org)$')
    urlcodes = ['.com', '.edu', '.gov', '.int', '.mil', '.net', \
            '.org', 'tgr.ph', '.ie', '.it', 'twurl.nl', \
            '.ly', '.co', '.uk', '.in', '.bg', '.me']

    url = None
    surl = simple_url_re.search(text)
    surl2 = simple_url_2_re.search(text)
    if surl:
        print simple_url_re.sub('URL', text)
        url = surl.group(0)
        print url
    elif surl2:
        url = surl2.group(0)
    else:
        for i in xrange(len(words)):
            for uc in urlcodes:
                if uc in words[i]:
                    words[i] = 'URL'
    return url

if __name__ == '__main__':
    a = 'Rhema #Vaithianathan - Are Bad Managers To Blame For NZ\'s Poor Economic Performance? yhoo.it/ypM4yy  #via @scoopnz'
    parse_tweet2(a)
    check_for_url(a)
    b = 'hello my name is joooooohnnnn'
    print remove_multiple_repeats(b)
