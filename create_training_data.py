import sqlite3
import pandas as pd

tables = ['2018-06']

#For the run through here, I am just running through a single month, having created only one database,
# but you might instead want to create a single database, with tables being called the month and year,
# or you can create a bunch of sqlite databases with tables like we have, then iterate them to create your files.

for table in tables:
    connection = sqlite3.connect('{}.db'.format(table))
    c = connection.cursor()
    # The first line just establishes our connection, then we define the cursor, then the limit.
    # The limit is the size of chunk that we're going to pull at a time from the database.
    # Again, we're working with data that is plausibly much larger than the RAM we have.
    # We want to set limit to 5000 for now, so we can have some testing data.
    limit = 5000
    last_unix = 0 #helps to the pull through the data.
    cur_length = limit
    counter = 0
    test_done = False

    connection.execute("SELECT * FROM parent_reply ORDER BY unix")

    # cur_length will tell us when we're done, counter will allow us to show some debugging information,
    # and test_done for when we're done building testing data.
    while cur_length == limit:
    #So long as the cur_length is the same as our limit, we've still got more pulling to do.

        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
        last_unix = df.tail(1)['unix'].values[0] #return value of tailing unix
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