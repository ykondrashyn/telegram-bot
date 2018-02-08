import sqlite3

"""
A class for interaction with SQLITE DB

"""


class DBsqlite(object):    

    def __init__(self, database='database.db', statements=None):
        self.database = database
        if statements is None:
            statements = []
        self.statement = ''
        self.display = False
        self.connect()
        self.execute(statements)
        self.close()            

    def connect(self):
        self.connection = sqlite3.connect(self.database)
        #self.connection.row_factory = lambda cursor, row: row[1]
        self.cursor = self.connection.cursor()
        self.connected = True

    def close(self): 
        self.connection.commit()
        self.connection.close()
        self.connected = False

    def rate(self, query):
        close = False
        if not self.connected:
            self.connect()
            close = True
        chat_id = query.message.chat_id
        message_id = query.message.message_id
        user_id = query.from_user.id
        chosen_emoji = query.data
        # change in to add_voter##self.add_user(query.from_user)
        try:
            # get reactions for message
            self.cursor.execute("SELECT messages.tmsg_id, reactions.description, result FROM rates \
				JOIN reactions ON rates.reaction_id = reactions.id \
				JOIN messages ON rates.message_id = messages.id \
				WHERE messages.tmsg_id=? AND messages.chat_id=(SELECT id FROM chats WHERE tchat_id=?) \
				AND reactions.description=?", (message_id, chat_id, chosen_emoji))
            reaction = self.cursor.fetchall()
            print (reaction)
            # write rates
            if not reaction:
                print ("INSIDE INSERT RATE SECTION")
                print("INSERT OR IGNORE INTO rates values (\
                (SELECT id FROM messages WHERE tmsg_id=?),\
                (SELECT id FROM reactions WHERE description=?),\
                (SELECT 1));, \
    		(message_id, chosen_emoji))", (message_id, chosen_emoji))
                self.cursor.execute("INSERT OR IGNORE INTO rates values (\
                (SELECT id FROM messages WHERE tmsg_id=?),\
                (SELECT id FROM reactions WHERE description=?),\
                (SELECT 1));", \
    		(message_id, chosen_emoji))
            else:
                print("UPDATING...")
                print("UPDATE rates SET result = result + 1 \
		WHERE message_id=((SELECT id from messages WHERE tmsg_id=?)) \
		AND reaction_id=(SELECT id from reactions WHERE description=?)", \
    		(message_id, chosen_emoji))
                self.cursor.execute("UPDATE rates SET result = result + 1 \
		WHERE message_id=((SELECT id from messages WHERE tmsg_id=?)) \
		AND reaction_id=(SELECT id from reactions WHERE description=?);", \
    		(message_id, chosen_emoji))

            # write voters
            #if not reaction:
            self.cursor.execute("INSERT OR IGNORE INTO voters values ( \
            (SELECT id from messages WHERE tmsg_id=?), \
            (SELECT ?), \
            (SELECT id from reactions WHERE description=?));", \
		(message_id, user_id, chosen_emoji))

	    ### Debug ###
            print(("reactions: {0}").format(reaction))
            self.cursor.execute("SELECT id FROM reactions WHERE description=?", (chosen_emoji,))
            bid = self.cursor.fetchall()
            print (bid)
	    ######

        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
            # TODO:
            # increment count in rates table for chosen reaction using description
            # add user to voters table
        print (query.message)
        #self.cursor.execute("INSERT INTO voters (id, tmsg_id, chat_id, user_id, from_user_id, forwarded_from_id)", (message_id, query.message.user_id, query.message.from_user_id))
        addposter = self.cursor.fetchall()

        """
        --SELECT messages.id, reactions.id from rates 
        --JOIN messages ON message_id = messages.id
        --JOIN reactions ON rates.reaction_id = reactions.id
        --WHERE messages.tmsg_id =997 AND messages.chat_id =-146733825 AND reactions.description="test128293"
        """
        if close:
            self.close()
        return 1


    def get_updated_keyboard(self, query):
        print('updated keyboard')
        close = False
        if not self.connected:
            self.connect()
            close = True
        chat_id = query.message.chat_id
        message_id = query.message.message_id
        user_id = query.from_user.id
        close = False
        if not self.connected:
            self.connect()
            close = True
        try:
            self.cursor.execute("\
SELECT t1.value, t1.description, t2.result \
FROM \
\
( \
                SELECT * FROM reactions \
) t1 \
\
LEFT JOIN \
\
( \
                SELECT reactions.id,  value, description, result \
                                FROM rates JOIN messages ON message_id = messages.id \
                                JOIN reactions ON rates.reaction_id = reactions.id \
                                WHERE messages.tmsg_id=? \
) t2 \
\
ON t1.id = t2.id", (message_id,))
#            self.cursor.execute("SELECT value, description, result \
#				FROM rates JOIN messages ON message_id = messages.id \
#				JOIN reactions ON rates.reaction_id = reactions.id \
#				WHERE messages.tmsg_id=?", (message_id,))
            data = self.cursor.fetchall()
        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()
        print('keyboard')
        print(data) 
        return data
 
    def get_keyboard(self):
        close = False
        if not self.connected:
            self.connect()
            close = True
        try:
            data = self.cursor.execute('SELECT value, description FROM reactions;').fetchall()
        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()
        print('keyboard')
        print(data) 
        return data 

    def register_message(self, message, message_sent):

        close = False
        if not self.connected:
            self.connect()
            close = True
        try:
            self.cursor.execute('INSERT INTO messages (id, tmsg_id, chat_id, user_id, forwarded_from_id) values \
				((?), (?), (SELECT id FROM chats WHERE tchat_id=?), \
				(SELECT id FROM users WHERE tuser_id=?), (?));', \
				(None, message_sent.message_id, message.chat.id, message.from_user.id, message.forward_from))
        except sqlite3.Error as error:
            print ('Terrible error has occurred:', error)
        # close connection if one was opened
        if close:
            self.close()   

    def register_user(self, user):
        close = False
        if not self.connected:
            self.connect()
            close = True
        try:
            self.cursor.execute("INSERT OR IGNORE INTO users (id, tuser_id, nickname, fname, lname) \
				values (?, ?, ?, ?, ?)", \
				(None, user.id, user.username, user.first_name, user.last_name)).fetchall()
        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()

    def register_chat(self, message):
        if not self.connected:
            self.connect()
            close = True
        try:
            self.cursor.execute('INSERT INTO chats (id, tchat_id, name, nickname, description) \
				values (?, ?, ?, ?, ?);', \
				(None, message.chat.id, message.chat.title, message.chat.username, message.chat.description))
        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()   

    def execute(self, statement, args=None):
        queries = []
        close = False
        if not self.connected:
            self.connect()
            close = True
        try:
            print(statement)
            if args:
                self.cursor.execute(statement, *args)
            else:
                self.cursor.execute(statement)
            #retrieve selected data
            data = self.cursor.fetchone()
            if statement.upper().startswith('SELECT'):
                #append query results
                queries.append(data)

        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
            print ('For the statement:', statement)
        # close connection if one was opened
        if close:
            self.close()   
        if self.display:      
            for result in queries:
                if result:
                    for row in result:
                        print (row)
                else:
                    print (result)
        else:
            return queries
