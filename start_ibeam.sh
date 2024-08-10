#!/bin/bash

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Change the current directory to the script's directory
cd "$SCRIPT_DIR"

# Now you are in the directory of the executing shell script

trade_account=$1

username=$(op read "op://trade/$trade_account/username")
password=$(op read "op://trade/$trade_account/password")
org=$(op read "op://trade/$trade_account/org")
port="50${trade_account: -2}"

echo -e "Stopping docker name "$org" ..."
docker stop "$org" > /dev/null 2>&1
echo -e "Removing docker name "$org" ..."
docker rm "$org" > /dev/null 2>&1

echo -e "Starting new IBEAM Gateway "$org" ..."
docker run -d --name "$org" --env IBEAM_ACCOUNT="$username" --env IBEAM_PASSWORD="$password" -p "$port":5000 voyz/ibeam

echo "IBEAM Gateway for $org was started with the container name $org at port $port"
