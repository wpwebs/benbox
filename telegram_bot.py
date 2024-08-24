#! .benbox/bin/python

import sys
import subprocess
from threading import Timer, Thread
import json
import logging
import requests
import time
from typing import Optional, Dict, Any, List, Union, Tuple  # Add Tuple to the import list
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters
from telegram.error import BadRequest, TimedOut
import urllib.parse
import html

# Set up logging
def setup_logger() -> logging.Logger:
    """Set up a logger that outputs INFO level to the console and DEBUG level to a file."""
    logger = logging.getLogger('telegram_bot')
    logger.setLevel(logging.DEBUG)  # Capture all log levels, but handlers will filter output

    # Console handler (INFO level)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (DEBUG level)
    file_handler = logging.FileHandler('telegram_bot_debug.log')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()

# Global cache dictionary
secret_cache: Dict[str, Tuple[str, float]] = {}
CACHE_TTL: int = 3600  # Cache time-to-live in seconds (e.g., 1 hour)


def cache_refresh_scheduler() -> None:
    """
    Periodically refreshes the cached secrets.
    """
    for vault_item in list(secret_cache.keys()):
        get_1password_secret(vault_item, force_refresh=True)
    # Schedule the next refresh
    Timer(CACHE_TTL / 2, cache_refresh_scheduler).start()

import re

def get_1password_secret(
    vault_item: str, retries: int = 5, backoff_factor: int = 2, force_refresh: bool = False
) -> Optional[str]:
    """
    Retrieve a secret from 1Password with retry logic and caching.
    
    :param vault_item: The 1Password item path (e.g., "op://vault/item").
    :param retries: The maximum number of retries in case of rate limiting.
    :param backoff_factor: The backoff multiplier for the retry delay.
    :param force_refresh: If True, forces a refresh of the cached secret.
    :return: The secret as a cleaned string or None if retrieval fails.
    """
    
    # Check if the secret is cached and still valid
    if not force_refresh:
        if vault_item in secret_cache:
            cached_secret, timestamp = secret_cache[vault_item]
            if time.time() - timestamp < CACHE_TTL:
                logger.debug(f"Using cached secret for {vault_item}")
                return cached_secret
            else:
                logger.debug(f"Cached secret for {vault_item} has expired. Fetching a new one.")
        else:
            logger.debug(f"No cached secret found for {vault_item}. Fetching a new one.")

    for attempt in range(retries):
        try:
            # Source the ~/.zshrc and capture the environment variables
            command = f"source ~/.zshrc && op read {vault_item}"
            logger.debug(f"Running command: {command}")

            result = subprocess.run(
                ["zsh", "-c", command],
                capture_output=True,
                text=True,
                check=True
            )

            secret = result.stdout.strip()
            logger.debug(f"Raw secret retrieved for {vault_item}: {repr(secret)}")

            # Remove lines that are not part of the secret (e.g., 'Agent pid' lines)
            cleaned_secret = re.sub(r'^.*Agent pid.*$', '', secret, flags=re.MULTILINE).strip()

            # Log the cleaned secret for debugging
            logger.debug(f"Cleaned secret for {vault_item}: {repr(cleaned_secret)}")

            # Validate the cleaned secret
            if not cleaned_secret:
                logger.error(f"Cleaned secret for {vault_item} is empty. This may indicate an issue with retrieval.")
                return None

            # Cache the cleaned secret
            secret_cache[vault_item] = (cleaned_secret, time.time())
            logger.debug(f"Secret for {vault_item} retrieved, cleaned, and cached.")

            return cleaned_secret

        except subprocess.CalledProcessError as e:
            if "rate-limited" in e.stderr.lower():
                wait_time = backoff_factor ** attempt
                logger.warning(f"Rate limit hit. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to retrieve the secret: {e}")
                return None

    logger.error(f"Failed to retrieve the secret after {retries} attempts.")
    return None

def escape_markdown_v2(text: str) -> str:
    """Escape special characters in text for MarkdownV2 format except for backticks."""
    escape_chars = r'_\[\]()~>#+-={}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

def send_message_to_topic(
    bot_token: str, chat_id: str, topic_id: str, message: str, chunk_size: int = 4096, retries: int = 3, timeout: int = 10, parse_mode: Optional[str] = "MarkdownV2"
) -> Optional[dict]:
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'

    # Escape the message if using MarkdownV2
    if parse_mode == "MarkdownV2":
        message = escape_markdown_v2(message)

    # Split the message into chunks if it exceeds the chunk size
    message_sections = message.split("\n\n")
    message_chunks = []
    current_chunk = ""

    for section in message_sections:
        if len(current_chunk) + len(section) + 2 <= chunk_size:
            current_chunk += f"{section}\n\n"
        else:
            message_chunks.append(current_chunk.strip())
            current_chunk = f"{section}\n\n"

    if current_chunk:
        message_chunks.append(current_chunk.strip())

    for attempt in range(retries):
        try:
            for chunk in message_chunks:
                params = {
                    'chat_id': chat_id,
                    'text': chunk,
                    'message_thread_id': topic_id,
                    'parse_mode': parse_mode
                }
                
                response = requests.post(url, data=params, timeout=timeout)
                response.raise_for_status()
                time.sleep(0.5)
            return response.json()
        except requests.HTTPError as e:
            logger.error(f"HTTPError on attempt {attempt + 1}: {e.response.text}")
            time.sleep(2)
        except requests.RequestException as e:
            logger.error(f"RequestException on attempt {attempt + 1}: {e}")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break
    return None

def delete_message(bot_token: str, chat_id: Union[str, int], topic_id: Union[str, int], message_id: int) -> bool:
    """
    Delete a specific message in a Telegram topic.
    
    :param bot_token: The Telegram bot token.
    :param chat_id: The chat ID where the message exists.
    :param topic_id: The topic ID within the chat.
    :param message_id: The ID of the message to delete.
    :return: True if the message was deleted, False otherwise.
    """
    url: str = f'https://api.telegram.org/bot{bot_token}/deleteMessage'
    params: Dict[str, Union[str, int]] = {
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

def delete_all_messages(bot_token: str, chat_id: Union[str, int], topic_id: Union[str, int]) -> None:
    """
    Delete all messages in a specific Telegram topic.
    
    :param bot_token: The Telegram bot token.
    :param chat_id: The chat ID where the messages exist.
    :param topic_id: The topic ID within the chat.
    """
    try:
        last_message_response: Optional[Dict[str, Any]] = send_message_to_topic(bot_token, chat_id, topic_id, 'Clearing topic ...')
        if not last_message_response or not last_message_response.get('ok'):
            logger.error(f"Failed to send clearing message: {last_message_response}")
            return

        message_id: int = last_message_response['result']['message_id']

        while message_id > 0:
            if not delete_message(bot_token, chat_id, topic_id, message_id):
                break
            message_id -= 1

    except Exception as e:
        logger.error(f"Failed to delete all messages: {e}")

async def handle_command(update: Update, context: CallbackContext, script_name: str, additional_args: Optional[List[str]] = None) -> None:
    chat_id: str = str(update.message.chat_id)
    topic_id: str = str(update.message.message_thread_id) if update.message.message_thread_id is not None else ''
    bot_token: str = context.bot.token
    args: List[str] = context.args if additional_args is None else context.args + additional_args

    # Add the chat ID, topic ID, and bot token to the command arguments
    args += [chat_id, topic_id, bot_token]

    command: str = script_name.replace("_handle.py", "")
    acknowledgment_message: str = f"Command `{command}` received. Executing..."

    # Send acknowledgment message
    send_message_to_topic(bot_token, chat_id, topic_id, acknowledgment_message, parse_mode='MarkdownV2')

    # Execute the command
    output: str = execute_command(script_name, args)

    if output:
        # Replace problematic characters
        output = output.replace("'", "").replace("[", "").replace("]", "")
        send_message_to_topic(bot_token, chat_id, topic_id, output, parse_mode=None)
    else:
        error_message: str = f"Command {command} executed, but no output was returned."
        error_message = error_message.replace("'", "").replace("[", "").replace("]", "")
        send_message_to_topic(bot_token, chat_id, topic_id, error_message, parse_mode=None)

def execute_command(script_name: str, args: List[str]) -> str:
    try:
        command = [sys.executable, script_name] + args
        result = subprocess.run(command, capture_output=True, text=True)
        result.check_returncode()  # This will raise CalledProcessError if the command fails
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Command {script_name} failed: {e}")
        return f"Error executing command {script_name}:\n{e}"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"Unexpected error executing command {script_name}:\n{e}"

async def handle_unknown_command(update: Update, context: CallbackContext) -> None:
    """
    Handle unknown commands by sending a warning message.
    
    :param update: The Telegram update object.
    :param context: The callback context object.
    """
    chat_id: str = str(update.message.chat_id)
    topic_id: str = str(update.message.message_thread_id)
    bot_token: str = context.bot.token

    unknown_command: str = update.message.text
    message: str = f"Unknown command {unknown_command}. Please use a valid command."
    logger.warning(f"Unknown command received: {unknown_command}")
    send_message_to_topic(bot_token, chat_id, topic_id, message)

async def handle_error(update: Optional[Update], context: CallbackContext) -> None:
    """
    Handle errors in command processing by notifying the user.
    
    :param update: The Telegram update object.
    :param context: The callback context object.
    """
    chat_id: Optional[str] = str(update.message.chat_id) if update and update.message else None
    topic_id: Optional[str] = str(update.message.message_thread_id) if update and update.message else None
    bot_token: Optional[str] = context.bot.token if context.bot else None

    if bot_token and chat_id and topic_id:
        message: str = "An error occurred while processing your command. Please try again later."
        send_message_to_topic(bot_token, chat_id, topic_id, message)
    
    logger.error(f"Error in command handling: {context.error}")

async def handle_clear(update: Update, context: CallbackContext) -> None:
    """
    Handle the /clear command by deleting all messages in the current topic.
    
    :param update: The Telegram update object.
    :param context: The callback context object.
    """
    logger.info("Received command /clear")
    chat_id: int = update.message.chat_id
    topic_id: int = update.message.message_thread_id
    bot_token: str = context.bot.token

    delete_all_messages(bot_token, chat_id, topic_id)

async def handle_info(THREAD_ID_TO_TOPIC_NAME: Dict[str, str], update: Update, context: CallbackContext) -> None:
    """
    Handle the /info command by providing details about the current topic.
    
    :param THREAD_ID_TO_TOPIC_NAME: Mapping from thread IDs to topic names.
    :param update: The Telegram update object.
    :param context: The callback context object.
    """
    logger.info("Received command /info")
    
    chat_id: int = update.message.chat_id
    chat_title: str = update.message.chat.title if update.message.chat.title else "No Title"
    thread_id: str = str(update.message.message_thread_id)
    allias_id: str = THREAD_ID_TO_TOPIC_NAME.get(thread_id, "Unknown Topic")
    strategy = get_1password_secret(f"op://trade/{allias_id}/strategy")    
    account_id = get_1password_secret(f"op://trade/{allias_id}/account")

    message: str = (
        f"*Topic Information:*\n"
        f"Channel Name: {chat_title}\n"
        f"Chat ID: `{chat_id}`\n"
        f"Thread ID: `{thread_id}`\n"
        f"Account: ||{account_id}||\n"
        f"Strategy: {strategy}"
    )
    
    response: Optional[Dict[str, Any]] = send_message_to_topic(
        context.bot.token, chat_id, thread_id, message, parse_mode='MarkdownV2'
    )
    if response and response.get('ok'):
        logger.info("Message sent successfully")
    else:
        logger.error("Failed to send message")

def main() -> None:
    """
    Main function to start the Telegram bot and register command handlers.
    """
    logger.info(f"Using Python interpreter: {sys.executable}")
    
    # Retrieve the secret as a string
    trading_group_secret: Optional[str] = get_1password_secret("op://dev/Telegrambot/trading")
    
    if trading_group_secret is None:
        logger.error("Failed to retrieve trading group information from 1Password or it's empty.")
        return
    
    # Log the type and content of trading_group_secret for debugging
    logger.debug(f"Raw trading group secret (length: {len(trading_group_secret)}): {repr(trading_group_secret)}")
    
    # Attempt to parse the secret as JSON to ensure it's a dictionary
    try:
        trading_group: Optional[Dict[str, str]] = json.loads(trading_group_secret)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode trading group JSON from 1Password secret 'op://dev/Telegrambot/trading': {e}")
        logger.debug(f"Content of the secret that caused the error: {repr(trading_group_secret)}")
        return
    
    if not isinstance(trading_group, dict):
        logger.error("The trading group data is not a dictionary.")
        return
    
    THREAD_ID_TO_TOPIC_NAME: Dict[str, str] = {v: k for k, v in trading_group.items()}
    logger.info(f"THREAD_ID_TO_TOPIC_NAME: {THREAD_ID_TO_TOPIC_NAME}")
    
    bot_token: Optional[str] = trading_group.get('token')

    if not bot_token:
        logger.error("Missing bot_token. Exiting.")
        return
    
    logger.info("Starting bot")
    application = Application.builder().token(bot_token).build()
    
    application.add_handler(CommandHandler('clear', handle_clear))
    application.add_handler(CommandHandler('info', lambda update, context: handle_info(THREAD_ID_TO_TOPIC_NAME, update, context)))
    
    screeners_command: str = "screeners"
    application.add_handler(CommandHandler(screeners_command, lambda update, context: handle_command(update, context, f"{screeners_command}_handle.py")))
    
    tickers_command: str = "tickers"
    application.add_handler(CommandHandler(tickers_command, lambda update, context: handle_command(update, context, f"{tickers_command}_handle.py")))
        
    gateway_command: str = "gateway"
    application.add_handler(CommandHandler(gateway_command, lambda update, context: handle_command(update, context, f"{gateway_command}_handle.py")))
    
    trade_command: str = "trade"
    application.add_handler(CommandHandler(trade_command, lambda update, context: handle_command(update, context, f"{trade_command}_handle.py")))

    application.add_handler(MessageHandler(filters.COMMAND, handle_unknown_command))
    application.add_error_handler(handle_error)
    
    logger.info("Bot is polling for updates")
    try:
        application.run_polling()
    except TimedOut as e:
        logger.error(f"Polling timed out: {e}")
    except BadRequest as e:
        logger.error(f"Bad request error during polling: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during polling: {e}")
    finally:
        logger.info("Bot is shutting down gracefully.")


if __name__ == '__main__':
    # Start the cache refresh scheduler in a separate thread
    Thread(target=cache_refresh_scheduler, daemon=True).start()
    
    # Start the main bot process
    main()
