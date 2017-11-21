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
        self.add_user(query.from_user)
        try:
            # get reactions for message
            self.cursor.execute("SELECT tmsg_id, description, result FROM rates JOIN reactions ON rates.reaction_id = reactions.id \
				JOIN messages ON message_id = messages.id \
				WHERE messages.tmsg_id = ? AND messages.chat_id = ?", (message_id, chat_id))
            reactions = self.cursor.fetchall()
            self.cursor.execute("SELECT id FROM reactions WHERE description=?", (chosen_emoji,))
            bid = self.cursor.fetchall()
            print (bid)

        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
            # TODO:
            # increment count in rates table for chosen reaction using description
            # add user to voters table
"""
INSERT INTO rates values (
(SELECT id from messages WHERE tmsg_id=997),
(SELECT id from reactions WHERE description="test128293"),
57)

--SELECT messages.id, reactions.id from rates 
--JOIN messages ON message_id = messages.id
--JOIN reactions ON rates.reaction_id = reactions.id
--WHERE messages.tmsg_id =997 AND messages.chat_id =-146733825 AND reactions.description="test128293"
"""

        if close:
            self.close()

    def add_user(self, user):
        close = False
        if not self.connected:
            self.connect()
            close = True
        try:
            self.cursor.execute("INSERT OR IGNORE INTO users (id, nickname, fname, lname) values (?, ?, ?, ?)",
					(user.id, user.username, user.first_name, user.last_name)).fetchall()
        except sqlite3.Error as error:
            print ('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()

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
        return data 

    def register_chat(self, chat_values):
        if not self.connected:
            self.connect()
            close = True
        try:
            self.cursor.execute('INSERT INTO chats (id, name, nickname, description) values (?, ?, ?, ?);', chat_values)
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
