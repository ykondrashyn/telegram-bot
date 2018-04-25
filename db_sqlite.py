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
        self.close()            

    def connect(self):
        self.connection = sqlite3.connect(self.database)
        self.connection.set_trace_callback(print)
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
            print(reaction)
            for reaction_id, react, count in reaction:
                logging.debug("SELECT * FROM voters WHERE tuser_id=? AND reaction_id=(SELECT id from reactions WHERE description=?);",
                                 (user_id, chosen_emoji))
                self.cursor.execute("SELECT * FROM voters WHERE tuser_id=? AND reaction_id=(SELECT id from reactions WHERE description=?);",
                                 (user_id, chosen_emoji))
                rate = self.cursor.fetchone()
                logging.debug("RATE: {}".format(rate))
            # write rates
            if not reaction:
                logging.debug("INSERT OR IGNORE INTO rates values (\
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
                print("SELECT reaction_id FROM voters WHERE tuser_id=? AND message_id=(SELECT id FROM messages WHERE tmsg_id=?)",
					(user_id, message_id))
                self.cursor.execute("SELECT reaction_id FROM voters WHERE tuser_id=? AND message_id=(SELECT id FROM messages WHERE tmsg_id=?)",
					(user_id, message_id))
                isinvoters = self.cursor.fetchone()
                
                logging.debug("UPDATING...")

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

###
#
#
#        rated_emoji = None
#        chosen_reaction_id = -1
#        rated_reaction_id = -1
#        res = {}
#        for reaction_id, reaction, count in reactions:
#            self.cur.execute("SELECT * FROM rates WHERE user_id=? AND reaction_id=?;",
#                             (user_id, reaction_id))
#            rate = self.cur.fetchone()
#            if rate:
#                rated_emoji = reaction
#                rated_reaction_id = reaction_id
#            if reaction == chosen_emoji:
#                chosen_reaction_id = reaction_id
#            res[reaction] = count
#
#        update_reaction_sql = "UPDATE reactions " \
#                              "SET count = %(count)s " \
#                              "WHERE id=%(reaction_id)s;"
#
#        if rated_emoji != chosen_emoji:
#            res[chosen_emoji] += 1
#            # increment count in reactions table for chosen reaction_id
#            self.cur.execute(update_reaction_sql, {'reaction_id': chosen_reaction_id,
#                                                   'count': res[chosen_emoji]})
#            # add user to rate table with chosen_emoji
#            self.cur.execute("INSERT INTO rates (user_id, reaction_id) VALUES "
#                             "(%(user_id)s, %(reaction_id)s)",
#                             {'user_id': user_id, 'reaction_id': chosen_reaction_id})
#
#        if rated_emoji:
#            res[rated_emoji] -= 1
#            # decrement count in reactions table for rated reaction_id
#            self.cur.execute(update_reaction_sql, {'reaction_id': rated_reaction_id,
#                                                   'count': res[rated_emoji]})
#            # remove user from rate with rated_emoji
#            self.cur.execute("DELETE FROM rates "
#                             "WHERE reaction_id=%(reaction_id)s",
#                             {'reaction_id': rated_reaction_id})
#        self.conn.commit()

	    ### Debug ###
            logging.debug(("reactions: {0}").format(reaction))
            self.cursor.execute("SELECT id FROM reactions WHERE description=?", (chosen_emoji,))
            bid = self.cursor.fetchall()
            logging.debug(bid)
	    ######

        except sqlite3.Error as error:
            logging.debug('An error occurred:', error.args[0])
            # TODO:
            # increment count in rates table for chosen reaction using description
            # add user to voters table
        logging.debug(query.message)
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
        return "You've voted \'" + "react" + "\'"


    def get_updated_keyboard(self, query):
        logging.debug('updated keyboard')
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
            logging.debug('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()
        logging.debug('keyboard')
        logging.debug(data) 
        return data
 
    def get_keyboard(self):
        close = False
        if not self.connected:
            self.connect()
            close = True
        try:
            data = self.cursor.execute('SELECT value, description FROM reactions;').fetchall()
        except sqlite3.Error as error:
            logging.debug('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()
        logging.debug('keyboard')
        logging.debug(data) 
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
            logging.debug('Terrible error has occurred:', error)
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
            logging.debug('An error occurred:', error.args[0])
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
