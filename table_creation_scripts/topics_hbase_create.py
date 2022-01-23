import happybase
connection = happybase.Connection(host='localhost', autoconnect=False, timeout=int(3e5))
connection.open()

TABLE_NAME = 'topics'
if TABLE_NAME in connection.tables():
    return
connection.create_table(TABLE_NAME, {'name':dict(), 
                                'article_stats':dict(), 
                                'twitter_stats':dict()})
connection.close()
