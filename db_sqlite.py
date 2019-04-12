import sqlite3
import logging

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

    def connect(self):
        self.connection = sqlite3.connect(self.database)
        #self.connection.row_factory = lambda cursor, row: row[1]
        self.cursor = self.connection.cursor()
        self.connection.set_trace_callback(print)
        self.connected = True

    def close(self): 
        self.connection.commit()
        self.connection.close()
        print("Closed")
        self.connected = False

    def rate(self, query):
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
            print(reaction)
            for reaction_id, react, count in reaction:
                print("SELECT * FROM voters WHERE user_id=? AND reaction_id=(SELECT id from reactions WHERE description=?);",
                                 (user_id, chosen_emoji))
                self.cursor.execute("SELECT * FROM voters WHERE user_id=? AND reaction_id=(SELECT id from reactions WHERE description=?);",
                                 (user_id, chosen_emoji))
                rate = self.cursor.fetchone()
                logging.debug("RATE: {}".format(rate))
            # write rates
            if not reaction:
                self.cursor.execute("INSERT OR IGNORE INTO rates values (\
                (SELECT id FROM messages WHERE tmsg_id=?),\
                (SELECT id FROM reactions WHERE description=?),\
                (SELECT 1));", \
            (message_id, chosen_emoji))
            else:
                print("SELECT reaction_id FROM voters WHERE user_id=? AND message_id=(SELECT id FROM messages WHERE tmsg_id=?);",
                    (user_id, message_id))
                self.cursor.execute("SELECT reaction_id FROM voters WHERE user_id=? AND message_id=(SELECT id FROM messages WHERE tmsg_id=?);",
                    (user_id, message_id))
                isinvoters = self.cursor.fetchone()
                
                logging.debug("UPDATING...")
                self.cursor.execute("SELECT result from rates WHERE message_id=(SELECT id FROM messages WHERE tmsg_id=?) \
                        AND reaction_id=(SELECT id from reactions WHERE description=?);", \
                        (message_id, chosen_emoji))
                result = self.cursor.fetchall()[0][0]
                print(f"========= {result} ==========")
                self.cursor.execute("UPDATE rates SET result=?+1 \
        WHERE message_id=(SELECT id from messages WHERE tmsg_id=?) \
        AND reaction_id=(SELECT id from reactions WHERE description=?);", \
                (result, message_id, chosen_emoji))

            # write voters
            #if not reaction:
            self.cursor.execute("INSERT OR IGNORE INTO voters values ( \
            (SELECT id from messages WHERE tmsg_id=?), \
            (SELECT ?), \
            (SELECT id from reactions WHERE description=?));", \
        (message_id, user_id, chosen_emoji))
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])
        logging.debug(query.message)
        addposter = self.cursor.fetchall()
        if close:
            self.close()
        return f"You've chosen \"{chosen_emoji}\""


    def get_updated_keyboard(self, query):
        logging.debug('updated keyboard')
        if not self.connected:
            self.connect()
            close = True
        chat_id = query.message.chat_id
        message_id = query.message.message_id
        user_id = query.from_user.id
        
        try:
            self.cursor.execute("\
                    SELECT t1.value, t1.description, t2.result FROM \
                    ( SELECT * FROM reactions ) t1 \
                    LEFT JOIN \
                    \
                    ( SELECT reactions.id,  value, description, result \
                    FROM rates JOIN messages ON message_id = messages.id \
                    JOIN reactions ON rates.reaction_id = reactions.id \
                    WHERE messages.tmsg_id=? ) t2 \
                    \
                    ON t1.id = t2.id", \
                    (message_id,))
            data = self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()
        logging.debug('keyboard')
        logging.debug(data) 
        return data
 
    def get_keyboard(self):
        
        if not self.connected:
            self.connect()
            close = True
        try:
            data = self.cursor.execute('SELECT value, description FROM reactions;').fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()
        logging.debug('keyboard')
        logging.debug(data) 
        return data 

    def register_message(self, message, message_sent):
        
        if not self.connected:
            self.connect()
            close = True
        try:
            print('INSERT INTO messages (id, tmsg_id, chat_id, user_id, forwarded_from_id) values \
                ((?), (?), (SELECT id FROM chats WHERE tchat_id=?), \
                (SELECT id FROM users WHERE tuser_id=?), (?));', \
                (None, message_sent.message_id, message.chat.id, message.from_user.id, message.forward_from))
            self.cursor.execute('INSERT INTO messages (id, tmsg_id, chat_id, user_id, forwarded_from_id) values \
                ((?), (?), (SELECT id FROM chats WHERE tchat_id=?), \
                (SELECT id FROM users WHERE tuser_id=?), (?));', \
                (None, message_sent.message_id, message.chat.id, message.from_user.id, message.forward_from)).fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('Terrible error has occurred:', error)
        # close connection if one was opened
        if close:
            self.close()   

    def register_user(self, user):
        
        if not self.connected:
            self.connect()
            close = True
        try:
            print("INSERT OR IGNORE INTO users (id, tuser_id, nickname, fname, lname) \
                values (?, ?, ?, ?, ?)", \
                (None, user.id, user.username, user.first_name, user.last_name))
            self.cursor.execute("INSERT OR IGNORE INTO users (id, tuser_id, nickname, fname, lname) \
                values (?, ?, ?, ?, ?)", \
                (None, user.id, user.username, user.first_name, user.last_name)).fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])
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
            self.close()
            logging.debug('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()   

    def execute(self, statement, args=None):
        queries = []
        close = True
        if not self.connected:
               self.connect()
               close = True
        try:
            logging.debug(statement)
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
            self.close()
            logging.debug('An error occurred:', error.args[0])
            logging.debug('For the statement:', statement)
        # close connection if one was opened
        if close:
            self.close()   
        if self.display:      
            for result in queries:
                if result:
                    for row in result:
                        logging.debug(row)
                else:
                    logging.debug(result)
        else:
            return queries
