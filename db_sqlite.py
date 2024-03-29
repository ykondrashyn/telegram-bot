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

    def __getreactions(self, chat_id, message_id, user_id, chosen_emoji):
        try:
            self.cursor.execute(
            """
            SELECT messages.tmsg_id, reactions.description, result FROM rates \
                JOIN reactions ON rates.reaction_id = reactions.id \
                JOIN messages ON rates.message_id = messages.id \
            WHERE messages.tmsg_id=? AND messages.chat_id=(SELECT id FROM chats WHERE tchat_id=?) \
                AND reactions.description=(?);
            """, (message_id, chat_id, chosen_emoji))
            
            reaction = self.cursor.fetchall()
            for reaction_id, react, count in reaction:
                self.cursor.execute(
            """
            SELECT * FROM voters WHERE user_id=(SELECT id FROM users WHERE tuser_id=?) AND reaction_id=(SELECT id FROM reactions WHERE description=?);
            """, (user_id, chosen_emoji))

            rate = self.cursor.fetchone()
            return reaction
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __writerates(self, message_id, user_id, chosen_emoji):
        try:
            logging.debug("Inserting new values...")
            self.cursor.execute(
            """
            INSERT OR IGNORE INTO rates values (\
            (SELECT id FROM messages WHERE tmsg_id=?),\
            (SELECT id FROM reactions WHERE description=?),\
            (SELECT 1));
            """, (message_id, chosen_emoji))
            return self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __inc_msg_rate(self, message_id, user_id, chosen_emoji):
        try:
            logging.debug("INCREMENTING...")
            self.cursor.execute(
            """
            UPDATE rates SET result=?+1 \
            WHERE message_id=(SELECT id FROM messages WHERE tmsg_id=?) \
            AND reaction_id=(SELECT id FROM reactions WHERE description=?);
            """,
            (self.__get_msg_rate(message_id, chosen_emoji), message_id, chosen_emoji))
            return self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __decr_msg_rate(self, message_id, user_id, chosen_emoji):
        try:
            logging.debug("DECREMENTING...")
            self.cursor.execute(
            """
            UPDATE rates SET result=?-1 \
            WHERE message_id=(SELECT id FROM messages WHERE tmsg_id=?) \
            AND reaction_id=(SELECT id FROM reactions WHERE description=?);
            """,
            (self.__get_msg_rate(message_id, chosen_emoji), message_id, chosen_emoji))
            return self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __updaterates(self, message_id, user_id, chosen_emoji, val):
        try:
            logging.debug("UPDATING...")
            self.cursor.execute(
            """
            UPDATE rates SET result=? \
            WHERE message_id=(SELECT id FROM messages WHERE tmsg_id=?) \
            AND reaction_id=(SELECT id FROM reactions WHERE description=?);
            """,
            (val, message_id, chosen_emoji))
            return self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __get_msg_rate(self, message_id, chosen_emoji):
        logging.debug("FETCHING RATE OF MESSAGE...")
        try:
            self.cursor.execute(
            """
            SELECT result FROM rates WHERE message_id=(SELECT id FROM messages WHERE tmsg_id=?) \
                AND reaction_id=(SELECT id FROM reactions WHERE description=?);
            """, (message_id, chosen_emoji))
            ret = self.cursor.fetchone()
            if ret:
                return ret[0]
            else:
                return 0
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])


    def __get_voter_reaction(self, message_id, user_id):
        logging.debug("FETCHING VOTER REACTION...")
        try:
            self.cursor.execute(
            """
            SELECT reaction_id FROM voters WHERE user_id=(SELECT id FROM users WHERE tuser_id=?) \
                AND message_id=(SELECT id FROM messages WHERE tmsg_id=?);
            """, (user_id, message_id))
            ret = self.cursor.fetchone()
            if ret:
                return ret[0]
            else:
                return 0
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __insert_voter(self, message_id, user_id, chosen_emoji):
        logging.debug("INSERTING VOTER...")
        try:
            self.cursor.execute(
            """
            INSERT INTO voters (message_id, user_id,reaction_id) values (\
                    (SELECT id FROM messages WHERE tmsg_id=?), (SELECT id FROM users WHERE tuser_id=?), \
                    (SELECT id FROM reactions WHERE description=?));
            """,
                (message_id, user_id, chosen_emoji)).fetchall()
            return self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __delete_msg_rate(self, message_id, chosen_emoji):
        logging.debug("DELETING MSG RATE...")
        try:
            self.cursor.execute(
            """
            DELETE FROM rates WHERE message_id=(SELECT id FROM messages WHERE tmsg_id=?) \
                    AND reaction_id=(SELECT id FROM reactions WHERE description=?);
            """,
                (message_id, chosen_emoji))
            return self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __delete_voter(self, message_id, user_id, chosen_emoji):
        logging.debug("DELETING VOTER...")
        try:
            self.cursor.execute(
            """
            DELETE FROM voters WHERE message_id=(SELECT id FROM messages WHERE tmsg_id=?) \
                    AND user_id=(SELECT id FROM users WHERE tuser_id=?) \
                    AND reaction_id=(SELECT id FROM reactions WHERE description=?);
            """,
                (message_id, user_id, chosen_emoji)).fetchall()
            return self.cursor.fetchall()
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])

    def __get_emoji_id_by_desc(self, chosen_emoji):
        self.cursor.execute(
        """
        SELECT id FROM reactions WHERE description=(?);
        """, (chosen_emoji,))
        ret = self.cursor.fetchone()
        if ret:
            return ret[0]
        else:
            return None
    def __get_emoji_desc_by_id(self, id):
        self.cursor.execute(
        """
        SELECT description FROM reactions WHERE id=(?);
        """, (id,))
        ret = self.cursor.fetchone()
        if ret:
            return ret[0]
        else:
            return None
# start
# 
#
    def rate(self, query):
        if not self.connected:
            self.connect()
            close = True
        chat_id = query.message.chat_id
        message_id = query.message.message_id
        user_id = query.from_user.id
        chosen_emoji = query.data
        try:
            # check if user voted already
            prev_vote_id = self.__get_voter_reaction(message_id, user_id)
            prev_vote_desc = self.__get_emoji_desc_by_id(prev_vote_id)
            print(f"!!!! {prev_vote_id}")
            print(f"!!!! {prev_vote_desc}")
            print(f"!!!! {chosen_emoji}")
            print((prev_vote_desc)==(chosen_emoji))
            rate = self.__get_msg_rate(message_id, chosen_emoji)
            print(f"!!!!\n RATE: {rate}")
            if prev_vote_id:
                if prev_vote_desc == chosen_emoji:
                    logging.debug("REMOVE VOTE")
                    self.__decr_msg_rate(message_id, user_id, chosen_emoji)
                    self.__delete_voter(message_id, user_id, chosen_emoji)
                else:
                    self.__decr_msg_rate(message_id, user_id, prev_vote_desc)
                    self.__delete_voter(message_id, user_id, prev_vote_desc)
                    self.__insert_voter(message_id, user_id, chosen_emoji)
                    if rate:
                        self.__inc_msg_rate(message_id, user_id, chosen_emoji)
                    else:
                        self.__writerates(message_id, user_id, chosen_emoji)

                rate = self.__get_msg_rate(message_id, chosen_emoji)
                rate_prev = self.__get_msg_rate(message_id, prev_vote_desc)
                if rate <=0:
                    self.__delete_msg_rate(message_id, chosen_emoji)
                if rate_prev <=0:
                    self.__delete_msg_rate(message_id, prev_vote_desc)
                if close:
                    self.close()
                    return ("You've removed your ", prev_vote_desc)
            logging.debug("NO PREV VOTES")
            # no one voted yet for this message_id, thus creating a new entry there
            self.__insert_voter(message_id, user_id, chosen_emoji)
            if rate:
                self.__inc_msg_rate(message_id, user_id, chosen_emoji)
            else:
                self.__writerates(message_id, user_id, chosen_emoji)
        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])
        logging.debug(query.message)
        if close:
            self.close()
        return ("You've chosen ", chosen_emoji)


    def get_updated_keyboard(self, query):
        logging.debug('updated keyboard')
        if not self.connected:
            self.connect()
            close = True
        chat_id = query.message.chat_id
        message_id = query.message.message_id
        user_id = query.from_user.id
        
        try:
            self.cursor.execute(
                    """
                    SELECT t1.value, t1.description, t2.result FROM \
                    ( SELECT * FROM reactions ) t1 \
                    LEFT JOIN \
                    \
                    ( SELECT reactions.id,  value, description, result \
                    FROM rates JOIN messages ON message_id = messages.id \
                    JOIN reactions ON rates.reaction_id = reactions.id \
                    WHERE messages.tmsg_id=? ) t2 \
                    \
                    ON t1.id = t2.id
                    """, (message_id,))

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
            data = self.cursor.execute(
            """
            SELECT value, description FROM reactions;
            """).fetchall()

        except sqlite3.Error as error:
            self.close()
            logging.debug('An error occurred:', error.args[0])
        # close connection if one was opened
        if close:
            self.close()
        return data 

    def register_message(self, message, message_sent):
        if not self.connected:
            self.connect()
            close = True
        try:
            self.cursor.execute(
            """
            INSERT INTO messages (id, tmsg_id, chat_id, user_id, forwarded_from_id) values \
                ((?), (?), (SELECT id FROM chats WHERE tchat_id=?), \
                (SELECT id FROM users WHERE tuser_id=?), (?));
            """, \
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
            self.cursor.execute(
            """
            INSERT OR IGNORE INTO users (id, tuser_id, nickname, fname, lname) \
                values (?, ?, ?, ?, ?)
            """, \
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
            self.cursor.execute(
            """
            INSERT INTO chats (id, tchat_id, name, nickname, description) \
                values (?, ?, ?, ?, ?);
            """, \
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
