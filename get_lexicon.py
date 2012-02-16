import re
import csv

def get_mpqa_lexicon(filename="mpqa_lexicon"):
    f = open(filename, 'rb')
    word_list = []
    for line in f:
        word = re.search(r"word1=(.*?) ", line).group(1)
        polarity = re.search(r"priorpolarity=(.+)", line).group(1)
        if polarity == 'negative':
            pscore = -1
        elif polarity == 'positive':
            pscore = 1
        else:
            pscore = 0
        word_list.append((word, pscore))
    return word_list

def create_mpqa_lexicon_csv(word_list, filename="mpqa_lexicon.csv", \
        pos_filename="mpqa_positive.csv", neg_filename="mpqa_negative.csv"):
    writer = csv.writer(open(filename, 'wb'))
    neg_writer = csv.writer(open(neg_filename, 'wb'))
    pos_writer = csv.writer(open(pos_filename, 'wb'))
    for tup in word_list:
        writer.writerow(tup)
        if tup[1] == -1:
            neg_writer.writerow([tup[0]])
        elif tup[1] == 1:
            pos_writer.writerow([tup[0]])

if __name__ == '__main__':
    word_list = get_mpqa_lexicon()
    print [word_list[i] for i in xrange(10)]
    create_mpqa_lexicon_csv(word_list)
