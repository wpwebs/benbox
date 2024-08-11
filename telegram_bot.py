#! .benbox/bin/python

import sys
import subprocess

import json
import logging
import requests
import time
from datetime import date, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters


# Set up logging
class SimpleUpdateFilter(logging.Filter):
    def filter(self, record):
        if "HTTP Request: POST" in record.getMessage() and "/getUpdates" in record.getMessage():
            record.msg = "Bot updating"
            record.args = ()
        return True

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger()
httpx_logger = logging.getLogger('httpx')
httpx_logger.addFilter(SimpleUpdateFilter())

def send_message_to_topic(bot_token, chat_id, topic_id, message):
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {
        'chat_id': chat_id,
        'parse_mode': 'Markdown',
        'text': message,
        'message_thread_id': topic_id
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to send message to topic: {e}")
        return None

def delete_message(bot_token, chat_id, topic_id, message_id):
    url = f'https://api.telegram.org/bot{bot_token}/deleteMessage'
    params = {
        'chat_id': chat_id,
        'message_thread_id': topic_id,
        'message_id': message_id
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('ok', False)
    except requests.RequestException as e:
        logger.error(f"Failed to delete message: {e}")
        return False

def delete_all_messages(bot_token, chat_id, topic_id):
    try:
        message_id = send_message_to_topic(bot_token, chat_id, topic_id, 'Last message')['result']['message_id']
        while delete_message(bot_token, chat_id, topic_id, message_id):
            message_id -= 1
    except Exception as e:
        logger.error(f"Failed to delete all messages: {e}")

def get_1password_secret(vault_item):
    import os
    
    try:
        result = subprocess.run(
            ["zsh", "-c", "source ~/.zshrc && env"],
            capture_output=True,
            text=True,
            check=True
        )
        env_vars = dict(line.split("=", 1) for line in result.stdout.splitlines() if '=' in line)
        op_token = env_vars.get('OP_SERVICE_ACCOUNT_TOKEN')
        if not op_token:
            raise EnvironmentError("OP_SERVICE_ACCOUNT_TOKEN is not set in the environment variables.")
        result = subprocess.run(
            ["op", "read", vault_item],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, **env_vars}
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to retrieve the secret: {e}")
        return None
    except EnvironmentError as env_err:
        logger.error(f"Environment Error: {env_err}")
        return None
    except Exception as ex:
        logger.error(f"An unexpected error occurred: {ex}")
        return None

async def handle_unknown_command(update: Update, context: CallbackContext):
    logger.info(f"Received unknown command: {update.message.text}")
    chat_id = update.message.chat_id
    topic_id = update.message.message_thread_id
    bot_token = context.bot.token
    
    error_message = "Unrecognized command. Please use a valid command."
    logger.info(error_message)
    send_message_to_topic(bot_token, chat_id, topic_id, error_message)
    

async def handle_clear(update: Update, context: CallbackContext):
    logger.info("Received command /clear")
    chat_id = update.message.chat_id
    topic_id = update.message.message_thread_id
    bot_token = context.bot.token

    delete_all_messages(bot_token, chat_id, topic_id)

async def handle_info(THREAD_ID_TO_TOPIC_NAME: dict, update: Update, context: CallbackContext):
    logger.info("Received command /info")
    
    chat_id = update.message.chat_id
    chat_title = update.message.chat.title if update.message.chat.title else "No Title"
    thread_id = str(update.message.message_thread_id)  # Ensure thread_id is a string
    topic_name = THREAD_ID_TO_TOPIC_NAME.get(thread_id, "Unknown Topic")

    message = f"**Topic Information:**\nChannel Name: {chat_title}\nChat ID: {chat_id}\nThread ID: {thread_id}\nTopic Name: {topic_name}"
    
    response = send_message_to_topic(context.bot.token, chat_id, thread_id, message)
    if response and response.get('ok'):
        logger.info("Message sent successfully")
    else:
        logger.error("Failed to send message")


    # scan_time = time.strftime('%H:%M %a, %Y/%m/%d')
    
async def handle_ratings(update: Update, context: CallbackContext):
    logger.info("Received command /ratings")
    chat_id = update.message.chat_id
    topic_id = update.message.message_thread_id
    bot_token = context.bot.token

    try:
        # Execute the ratings.py script
        result = subprocess.run(["./ratings.py"], capture_output=True, text=True)
        result.check_returncode()  # Raise an error if the command failed

        output = result.stdout.strip() or "No output from the ratings script."
        logger.info(f"Ratings script executed successfully with output: {output}")

        # Send the script's output to the Telegram topic
        send_message_to_topic(bot_token, chat_id, topic_id, f"**Ratings Results:**\n```\n{output}\n```")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to execute ratings.py: {e}")
        send_message_to_topic(bot_token, chat_id, topic_id, "Failed to execute ratings script.")
    except Exception as ex:
        logger.error(f"An unexpected error occurred while executing ratings.py: {ex}")
        send_message_to_topic(bot_token, chat_id, topic_id, "An unexpected error occurred while executing ratings script.")

async def handle_sample(update: Update, context: CallbackContext):
    logger.info("Received command /sample")
    chat_id = update.message.chat_id
    topic_id = update.message.message_thread_id
    bot_token = context.bot.token

    if len(context.args) < 1:
        send_message_to_topic(bot_token, chat_id, topic_id, "Please specify a function and optional arguments: `/sample func1 [args]` or `/sample func2 [args]`.")
        return

    func_name = context.args[0]
    func_args = context.args[1:]  # Collect any additional arguments

    try:
        # Execute the sample.py script with the specified function and arguments
        command = ["./sample.py", func_name] + func_args
        result = subprocess.run(command, capture_output=True, text=True)
        result.check_returncode()  # Raise an error if the command failed

        output = result.stdout.strip() or f"No output from {func_name}."
        logger.info(f"sample.py {func_name} executed successfully with output: {output}")

        # Send the script's output to the Telegram topic
        send_message_to_topic(bot_token, chat_id, topic_id, f"**{func_name.capitalize()} Results:**\n```\n{output}\n```")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to execute {func_name} in sample.py: {e}")
        send_message_to_topic(bot_token, chat_id, topic_id, f"Failed to execute {func_name} in the sample script.")
    except Exception as ex:
        logger.error(f"An unexpected error occurred while executing {func_name} in sample.py: {ex}")
        send_message_to_topic(bot_token, chat_id, topic_id, f"An unexpected error occurred while executing {func_name} in the sample script.")

def main():
    logger.info(f"Using Python interpreter: {sys.executable}")
    
    trading_group_secret = get_1password_secret("op://dev/Telegrambot/trading")
    trading_group = json.loads(trading_group_secret) if trading_group_secret else None
    
    # Create a reverse mapping from thread_id to topic_name
    THREAD_ID_TO_TOPIC_NAME = {v: k for k, v in trading_group.items()}
    logger.info(THREAD_ID_TO_TOPIC_NAME)
    
    if not trading_group:
        logger.error("Failed to retrieve trading group information from 1Password")
        return
    
    bot_token = trading_group.get('token')

    if not all([bot_token]):
        logger.error("Missing bot_token. Exiting.")
        return
    
    logger.info("Starting bot")
    application = Application.builder().token(bot_token).build()
    
    # Handler for unknown commands
    application.add_handler(MessageHandler(filters.COMMAND, handle_unknown_command))
    
    # Handler for clear command that clear all previous posts
    application.add_handler(CommandHandler('clear', handle_clear))

    # Add the info handler, passing the THREAD_ID_TO_TOPIC_NAME
    application.add_handler(CommandHandler('info', lambda update, context: handle_info(THREAD_ID_TO_TOPIC_NAME, update, context)))

    application.add_handler(CommandHandler('ratings', handle_ratings))
    application.add_handler(CommandHandler('sample', handle_sample))

    logger.info("Bot is polling for updates")
    application.run_polling()

if __name__ == '__main__':
    main()
