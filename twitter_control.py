import twitter


if __name__ == '__main__':
    api = twitter.Api()
    statuses = api.GetPublicTimeline(500)
    userlist = [s.user.name for s in statuses]
    print 'User List Length: ', len(userlist)


    status_dic = {}
    NoError = True
    i = 1
    total_repeats = 0
    num_repeats = 0
    while NoError:
        try: 
            search = api.GetSearch('economy', per_page = 100, page=i)
            print 'Search List Length: ', len(search)
            for s in search:
                if s.id in status_dic:
                    total_repeats += 1
                    if s.text == status_dic[s.id]: num_repeats += 1
                else:
                    status_dic[s.id] = s.text

        except twitter.TwitterError:
            NoError = False
        
        i += 1
    print total_repeats, num_repeats

