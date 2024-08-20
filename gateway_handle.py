#! .benbox/bin/python

import subprocess
import logging
import sys
import json

from telegram_trade import get_1password_secret

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_credentials(account_id):
    """Retrieve credentials for a given account."""
    try:
        org = get_1password_secret(f"op://trade/{account_id}/org")
        master_acc = get_1password_secret(f"op://trade/{account_id}/gateway")
        port = f'50{master_acc[-2:]}'
        username = get_1password_secret(f"op://trade/{master_acc}/username")
        password = get_1password_secret(f"op://trade/{master_acc}/password")
        return username, password, port, org
    except Exception as e:
        logger.error(f"Failed to retrieve credentials for account ID '{account_id}': {e}")
        return None, None, None, None

def is_container_running(org):
    """Check if the Docker container is running."""
    try:
        result = subprocess.run(['docker', 'ps', '-f', f'name={org}', '--format', '{{.Names}}'], capture_output=True, text=True)
        return org in result.stdout.splitlines()
    except Exception as e:
        logger.error(f"Error checking if container '{org}' is running: {e}")
        return False

def manage_docker_container(account_id, action, force=False):
    """Start, stop, or check the status of the Docker container based on the action."""
    if account_id is None:
        return show_all_containers()

    username, password, port, org = get_credentials(account_id)
    if org is None:
        return f"Error retrieving credentials: Account ID '{account_id}' not found.\nPlease execute the command at account topic."

    if action == 'start':
        if is_container_running(org):
            if not force:
                logger.info(f"IBEAM Gateway for {account_id} is already running with the container name '{org}' at port {port}. No action taken.")
                return f"IBEAM Gateway for {account_id} is already running with the container name '{org}' at port {port}. Use 'start force' to restart."
            else:
                logger.info(f"Force restarting the IBEAM Gateway for {account_id}...")

        try:
            logger.info(f"Stopping and removing any existing container named {org} ...")
            subprocess.run(['docker', 'stop', org], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(['docker', 'rm', org], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            logger.info(f"Starting new IBEAM Gateway {org} ...")
            result = subprocess.run([
                'docker', 'run', '-d', '--name', org,
                '--env', f'IBEAM_ACCOUNT={username}',
                '--env', f'IBEAM_PASSWORD={password}',
                '-p', f'{port}:5000', 'voyz/ibeam'
            ], capture_output=True)

            if result.returncode == 0:
                logger.info(f"IBEAM Gateway for {account_id} started with the container name {org} at port {port}")
                return f"IBEAM Gateway for {account_id} started successfully with container name '{org}' at port {port}."
            else:
                logger.error(f"Failed to start IBEAM Gateway for {account_id} with the container name {org} at port {port}.")
                return f"Failed to start IBEAM Gateway for {account_id} with the container name '{org}' at port {port}."
        except Exception as e:
            logger.error(f"Failed to start IBEAM Gateway for {account_id}: {e}")
            return f"Failed to start IBEAM Gateway for {account_id}: {e}"

    elif action == 'stop':
        try:
            logger.info(f"Stopping docker container named {org} ...")
            result = subprocess.run(['docker', 'stop', org], capture_output=True)
            subprocess.run(['docker', 'rm', org], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            if result.returncode == 0:
                logger.info(f"IBEAM Gateway for {account_id} was stopped.")
                return f"IBEAM Gateway for {account_id} was stopped successfully."
            else:
                logger.error(f"Failed to stop IBEAM Gateway for {account_id}.")
                return f"Failed to stop IBEAM Gateway for {account_id}."
        except Exception as e:
            logger.error(f"Failed to stop IBEAM Gateway for {account_id}: {e}")
            return f"Failed to stop IBEAM Gateway for {account_id}: {e}"

    elif action == 'status':
        try:
            logger.info(f"Checking status of docker container named {org} ...")
            if is_container_running(org):
                logger.info(f"IBEAM Gateway for {account_id} is currently running with the container name {org} at port {port}.")
                return f"IBEAM Gateway for {account_id} is currently running with the container name '{org}' at port {port}."
            else:
                logger.info(f"IBEAM Gateway for {account_id} is not running.")
                return f"IBEAM Gateway for {account_id} is not running."
        except Exception as e:
            logger.error(f"Failed to check status of IBEAM Gateway for {account_id}: {e}")
            return f"Failed to check status of IBEAM Gateway for {account_id}: {e}"

    else:
        raise ValueError(f"Unknown action: {action}")

def show_all_containers():
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

def main(args):
    if len(args) < 1:
        logger.info("No arguments provided. Showing all running Docker containers.")
        print(show_all_containers())
        sys.exit(0)

    try:
        func_name = args[0]
        topic_id = args[2] if len(args) > 2 else None
        force = args[1].lower() == 'force' if len(args) > 1 else False

        # Mapping the topic_id with account_id
        trading_group_secret = get_1password_secret("op://dev/Telegrambot/trading")
        trading_group = json.loads(trading_group_secret) if trading_group_secret else None        
        THREAD_ID_TO_TOPIC_NAME = {v: k for k, v in trading_group.items()}

        account_id = THREAD_ID_TO_TOPIC_NAME.get(str(topic_id), None)

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
