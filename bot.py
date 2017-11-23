# -*- coding: utf-8 -*-

from db_sqlite import DBsqlite
from telegram import Bot, Update, Message
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler
import logging
from config import *

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram import CallbackQuery

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

# define sqlite setup statements
sqlitesetupstatements = "PRAGMA foreign_keys = ON;"
db = DBsqlite(database, sqlitesetupstatements)


# Define a few command handlers. These usually take the two arguments bot and
# update. Error handlers also receive the raised TelegramError object in error.

def button_callback(bot, update):
    query = update.callback_query
    message = query.message
    res = db.rate(query)
#    if res:
#        original_msg = db.original_message(query)
#        reply_markup = get_buttons_markup(original_msg, res)
#        bot.edit_message_reply_markup(chat_id=message.chat_id, message_id=message.message_id, reply_markup=reply_markup)




def get_buttons_markup():
    keys = []
    keyboard = []
    max_cols = 4
    buttons = db.get_keyboard()
    for button in buttons:
        text = chr(button[0]) + chr(button[2])
        name = button[1]
        keys.append(InlineKeyboardButton(text, callback_data=name))
    while keys:
        keyboard += [keys[:max_cols]]
        keys = keys[max_cols:]
    return InlineKeyboardMarkup(keyboard)

def start(bot, update):
    update.message.reply_text("Bot has started\n" + update.message.status_update.new_chat_members)

def joined(bot, update):
    bot.send_message(update.message.chat_id,'I am joined' + str(update.message.new_chat_members))
    print(*update.message.new_chat_members)
    db.register_chat(update.message)

def error(bot, update, error):
    logger.warn('Update "%s" caused error "%s"'.format(update, error))

def resend_message(bot, update):
    resend = True
    message = update.message
    if message.reply_to_message:
        return
    if message.photo:
        message_sent = bot.send_photo(chat_id = message.chat_id, photo=message.photo[0].file_id, disable_notification = True, reply_markup=reply_markup)
        db.register_user(message.from_user)
        db.register_message(message, message_sent)
    else:
        resend = False
    if resend:
        message.delete()

def main():
    # Create the EventHandler and pass it your bot's token.
    updater = Updater(token)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # We joined a chat
    # 
    print(db.get_keyboard())
    get_buttons_markup()
    global reply_markup
    reply_markup = get_buttons_markup()
    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_error_handler(error)
    msghandler = MessageHandler(Filters.all, resend_message)
    new_members_handler = MessageHandler(Filters.status_update.new_chat_members, joined)
    dp.add_handler(new_members_handler)
    dp.add_handler(msghandler)
    dp.add_handler(CallbackQueryHandler(button_callback))
    # To be replaced with Webhook method
    updater.start_polling()

    updater.idle()


if __name__ == '__main__':
    main()
