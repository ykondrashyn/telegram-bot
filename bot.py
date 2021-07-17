# -*- coding: utf-8 -*-

from db_sqlite import DBsqlite
from telegram import Bot, Update, Message
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler
import logging
from config import *

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram import CallbackQuery

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

# define sqlite setup statements
sqlitesetupstatements = "PRAGMA foreign_keys = ON;"
db = DBsqlite(database, sqlitesetupstatements)


# Define a few command handlers. These usually take the two arguments bot and
# update. Error handlers also receive the raised TelegramError object in error.

def button_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    message = query.message
    db.register_user(query.from_user)
    res = db.rate(query)
    if res:
        reply_markup = get_updated_buttons_markup(query)
        context.bot.edit_message_reply_markup(chat_id=message.chat_id, message_id=message.message_id, reply_markup=reply_markup)
    context.bot.answer_callback_query(callback_query_id=query.id, show_alert=False, text=res)

def get_updated_buttons_markup(query):
    keys = []
    keyboard = []
    max_cols = 4
    buttons = db.get_updated_keyboard(query)
    for button in buttons:
        button = ['' if x is None else x for x in button]
        text =  f"{button[0]} {button[2]}"
        name = button[1]
        keys.append(InlineKeyboardButton(text, callback_data=name))
    while keys:
        keyboard += [keys[:max_cols]]
        keys = keys[max_cols:]
    return InlineKeyboardMarkup(keyboard)

def get_buttons_markup():
    keys = []
    keyboard = []
    max_cols = 4
    buttons = db.get_keyboard()
    for button in buttons:
        text = button[0]
        name = button[1]
        keys.append(InlineKeyboardButton(text, callback_data=name))
    while keys:
        keyboard += [keys[:max_cols]]
        keys = keys[max_cols:]
    return InlineKeyboardMarkup(keyboard)

def start(update: Update, context: CallbackContext)-> None:
    update.message.reply_text("Bot has started\n" + update.message.status_update.new_chat_members)

def joined(update: Update, context: CallbackContext):
    context.bot.send_message(update.message.chat_id,'I am joined' + str(update.message.new_chat_members))
    db.register_chat(update.message)

def error(update: Update, context: CallbackContext):
    logger.warning(f"Update {update} caused error {context.error}")

def resend_message(update: Update, context: CallbackContext):
    resend = True
    message = update.message
    if message.reply_to_message:
        return
    if message.photo:
        message_sent = context.bot.send_photo(chat_id = message.chat_id, photo=message.photo[0].file_id, disable_notification = True, reply_markup=reply_markup)
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

    logger.debug("Reactions fetched from db:\n{}".format(' '.join(map(str, db.get_keyboard()))))
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
