#! .benbox/bin/python

import subprocess
import logging
import sys
import json
from typing import Optional, Tuple, List
from telegram_bot import get_1password_secret


def setup_logger() -> logging.Logger:
    """Set up a logger that outputs INFO level to the console and DEBUG level to a file."""
    logger = logging.getLogger('gateway_handle')
    logger.setLevel(logging.DEBUG)

    # Ensure we don't add multiple handlers
    if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)  # Default to INFO level
        logger.addHandler(console_handler)
        logger.debug("Console handler added.")

    if not any(isinstance(handler, logging.FileHandler) for handler in logger.handlers):
        # File handler
        file_handler = logging.FileHandler('gateway_debug.log')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        logger.debug("File handler added.")

    return logger

def set_console_level(logger: logging.Logger, level: str) -> None:
    """Set the logging level of the console handler."""
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            if level.upper() == 'DEBUG':
                handler.setLevel(logging.DEBUG)
            elif level.upper() == 'INFO':
                handler.setLevel(logging.INFO)
            else:
                raise ValueError("Invalid level: choose 'DEBUG' or 'INFO'")
            logger.info(f"Console logging level set to {level.upper()}.")
            break

def disable_console_handler(logger: logging.Logger) -> None:
    """Remove the console handler from the logger."""
    handlers_to_remove = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
    for handler in handlers_to_remove:
        logger.removeHandler(handler)
        handler.close()
    logger.debug(f"Console handler removed. Remaining handlers: {logger.handlers}")
    logger.info("Console logging has been disabled.")

# Initialize the logger
logger = setup_logger()

def get_credentials(account_id: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Retrieve credentials for a given account."""
    try:
        container_name = get_1password_secret(f"op://trade/{account_id}/org", force_refresh=True)
        master_acc = get_1password_secret(f"op://trade/{account_id}/gateway", force_refresh=True)
        
        if not master_acc:
            logger.error(f"Retrieved master_acc for account '{account_id}' is None or empty.")
            return None, None, None, None
        
        logger.debug(f"Retrieved master_acc for account '{account_id}': {master_acc}")
        
        port = f'50{master_acc[-2:]}' if master_acc else None
        username = get_1password_secret(f"op://trade/{master_acc}/username")
        password = get_1password_secret(f"op://trade/{master_acc}/password")
        
        if not username:
            logger.error(f"Failed to retrieve username for account '{account_id}' using master_acc '{master_acc}'")
        if not password:
            logger.error(f"Failed to retrieve password for account '{account_id}' using master_acc '{master_acc}'")
        
        logger.debug(f"Retrieved credentials for account '{account_id}': container_name={container_name}, port={port}, username={username}, password={password}")
        
        return username, password, port, container_name
    except Exception as e:
        logger.error(f"Failed to retrieve credentials for account ID '{account_id}': {e}")
        return None, None, None, None

def is_container_running(container_name: str) -> bool:
    """Check if the Docker container for the given container name is running."""
    try:
        result = subprocess.run(
            ['docker', 'inspect', '--format={{.State.Running}}', container_name],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            is_running = result.stdout.strip().lower() == 'true'
            logger.info(f"Docker container '{container_name}' running status: {is_running}")
            return is_running
        else:
            logger.error(f"Failed to inspect the Docker container '{container_name}'. Return code: {result.returncode}")
            return False
    except Exception as e:
        logger.error(f"Error checking if container '{container_name}' is running: {e}")
        return False

def manage_docker_container(account_id: Optional[str], action: str, force: bool = False) -> str:
    """Start, stop, or check the status of the Docker container based on the action."""
    if account_id is None:
        return show_all_containers()

    username, password, port, container_name = get_credentials(account_id)
    if container_name is None:
        return f"Error retrieving credentials: Account ID '{account_id}' not found.\nPlease execute the command at account topic."

    try:
        if action in ['start', 'stop']:
            if is_container_running(container_name):
                logger.info(f"Stopping Docker container named '{container_name}' ...")
                stop_result = subprocess.run(['docker', 'stop', container_name], capture_output=True, text=True)
                if stop_result.returncode != 0:
                    logger.error(f"Failed to stop Docker container '{container_name}': {stop_result.stderr}")
                    return f"Failed to stop Docker container '{container_name}': {stop_result.stderr}"

                logger.info(f"Removing Docker container named '{container_name}' ...")
                rm_result = subprocess.run(['docker', 'rm', container_name], capture_output=True, text=True)
                if rm_result.returncode != 0:
                    logger.error(f"Failed to remove Docker container '{container_name}': {rm_result.stderr}")
                    return f"Failed to remove Docker container '{container_name}': {rm_result.stderr}"
                logger.info(f"Container '{container_name}' was stopped and removed successfully.")

        if action == 'start':
            logger.info(f"Starting new IBEAM Gateway '{container_name}' ...")
            run_result = subprocess.run([
                'docker', 'run', '-d', '--name', container_name,
                '--env', f'IBEAM_ACCOUNT={username}',
                '--env', f'IBEAM_PASSWORD={password}',
                '-p', f'{port}:5000', 'voyz/ibeam'
            ], capture_output=True, text=True)

            if run_result.returncode == 0:
                logger.info(f"IBEAM Gateway for {account_id} started with the container name '{container_name}' at port {port}.")
                return f"IBEAM Gateway for {account_id} started successfully with container name '{container_name}' at port {port}."
            else:
                logger.error(f"Failed to start IBEAM Gateway for {account_id} with the container name '{container_name}' at port {port}: {run_result.stderr}")
                return f"Failed to start IBEAM Gateway for {account_id} with the container name '{container_name}' at port {port}: {run_result.stderr}"

        elif action == 'stop':
            return f"IBEAM Gateway for {account_id} was stopped and removed successfully."

        elif action == 'status':
            logger.info(f"Checking status of Docker container named '{container_name}' ...")
            if is_container_running(container_name):
                logger.info(f"IBEAM Gateway for {account_id} is currently running with the container name '{container_name}' at port {port}.")
                return f"IBEAM Gateway for {account_id} is currently running with the container name '{container_name}' at port {port}."
            else:
                logger.info(f"IBEAM Gateway for {account_id} is not running.")
                return f"IBEAM Gateway for {account_id} is not running."

        else:
            logger.error(f"Unknown action: {action}")
            return f"Unknown action: {action}"

    except Exception as e:
        logger.error(f"Failed to manage IBEAM Gateway for {account_id}: {e}")
        return f"Failed to manage IBEAM Gateway for {account_id}: {e}"

def show_all_containers() -> str:
    """Show the status of all running Docker containers."""
    logger.info("Displaying all running Docker containers...")
    try:
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout
        else:
            logger.error("Failed to retrieve the list of running Docker containers.")
            return "Failed to retrieve the list of running Docker containers."
    except Exception as e:
        logger.error(f"Error retrieving Docker container list: {e}")
        return f"Error retrieving Docker container list: {e}"

def main(args: List[str]) -> None:
    """Main function to handle command-line arguments and execute corresponding actions."""
    if len(args) < 1:
        logger.info("No arguments provided. Showing all running Docker containers.")
        print(show_all_containers())
        sys.exit(0)

    try:
        func_name = args[0].lower()
        if func_name == 'logger' and len(args) > 1:
            logger_command = args[1].lower()
            if logger_command == 'info':
                set_console_level(logger, 'INFO')
            elif logger_command == 'debug':
                set_console_level(logger, 'DEBUG')
            elif logger_command == 'disable':
                disable_console_handler(logger)
            else:
                print(f"Unknown logger command: {logger_command}")
            sys.exit(0)

        topic_id = args[2] if len(args) > 2 else None
        force = args[1].lower() == 'force' if len(args) > 1 else False

        trading_group_secret = get_1password_secret("op://dev/Telegrambot/trading")
        trading_group = json.loads(trading_group_secret) if trading_group_secret else None        
        THREAD_ID_TO_TOPIC_NAME = {v: k for k, v in trading_group.items()}

        account_id = THREAD_ID_TO_TOPIC_NAME.get(str(topic_id), None)

        logger.debug(f"Function: {func_name}, Account ID: {account_id}, Force: {force}")

        if func_name == 'start':
            message = manage_docker_container(account_id, 'start', force)
        elif func_name == 'stop':
            message = manage_docker_container(account_id, 'stop')
        elif func_name == 'status':
            message = manage_docker_container(account_id, 'status')
        else:
            logger.debug(f"Function '{func_name}' not found. Showing all running Docker containers.")
            message = show_all_containers()
        
        print(message)

    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        print(f"Error: {ex}")

if __name__ == "__main__":
    main(sys.argv[1:])