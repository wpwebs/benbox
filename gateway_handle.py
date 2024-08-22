#!/benbox/bin/python

import subprocess
import logging
import sys
import json
from typing import Optional, Tuple, List

from telegram_bot import get_1password_secret

# Set up logging specifically for this script
logger = logging.getLogger('gateway_handle')
logger.setLevel(logging.INFO)  # Set to DEBUG to capture all levels of logs

# Create a console handler and set the level to DEBUG
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Create a formatter and set it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)

def get_credentials(account_id: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Retrieve credentials for a given account.

    :param account_id: The account ID for which credentials are being retrieved.
    :return: A tuple containing the username, password, port, and container name.
    """
    try:
        container_name = get_1password_secret(f"op://trade/{account_id}/org",force_refresh=True)
        master_acc = get_1password_secret(f"op://trade/{account_id}/gateway",force_refresh=True)
        
        # Log and check for None or empty master_acc
        if not master_acc:
            logger.error(f"Retrieved master_acc for account '{account_id}' is None or empty.")
            return None, None, None, None
        
        logger.debug(f"Retrieved master_acc for account '{account_id}': {master_acc}")
        
        port = f'50{master_acc[-2:]}' if master_acc else None
        username = get_1password_secret(f"op://trade/{master_acc}/username")
        password = get_1password_secret(f"op://trade/{master_acc}/password")
        
        # Log if any retrieved value is None
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
    """
    Check if the Docker container for the given container name is running.

    :param container_name: The name of the Docker container.
    :return: True if the container is running, False otherwise.
    """
    try:
        result = subprocess.run(
            ['docker', 'inspect', '--format={{.State.Running}}', container_name],
            capture_output=True,
            text=True
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
    """
    Start, stop, or check the status of the Docker container based on the action.

    :param account_id: The account ID associated with the container.
    :param action: The action to perform ('start', 'stop', or 'status').
    :param force: Whether to force the start of the container.
    :return: A message indicating the result of the action.
    """
    if account_id is None:
        return show_all_containers()

    username, password, port, container_name = get_credentials(account_id)
    if container_name is None:
        return f"Error retrieving credentials: Account ID '{account_id}' not found.\nPlease execute the command at account topic."

    try:
        if action in ['start', 'stop']:
            # Stop and remove the container if it exists
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
    """
    Show the status of all running Docker containers.

    :return: A string listing all running Docker containers.
    """
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
    """
    Main function to handle command-line arguments and execute corresponding actions.

    :param args: Command-line arguments.
    """
    if len(args) < 1:
        logger.info("No arguments provided. Showing all running Docker containers.")
        print(show_all_containers())
        sys.exit(0)

    try:
        func_name = args[0].lower()
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
            logger.error(f"Function '{func_name}' not found. Showing all running Docker containers.")
            message = show_all_containers()
        
        print(message)

    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        print(f"Error: {ex}")

if __name__ == "__main__":
    main(sys.argv[1:])
