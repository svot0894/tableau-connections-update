import yaml
import argparse
import requests
from datetime import datetime

from helpers.tableau_server_funcs import (
    sign_in,
    sign_out,
    get_objects,
    get_connections,
    update_connection
)

# set up argument parser
parser = argparse.ArgumentParser(
    description="Process an indicator argument."
)
parser.add_argument(
    "-i",
    "--indicator",
    type=str,
    choices=["snowflake", "databricks", "oracle"],
    help="Tells the tool which database connection to update."
)
parser.add_argument(
    "-e",
    "--environment",
    type=str,
    choices=["dev", "tst", "stg", "prd"],
    help="Tells the tool which environment to update."
)
args = parser.parse_args()

# validate arguments
if not args.indicator or not args.environment:
    raise ValueError("You must provide both indicator and environment arguments using -i and -e flags.")

# import yaml configs
config_file_path = f"configs/{args.environment}/config.yml"

with open(config_file_path, "r") as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

# logging function
def log(message: str) -> None:
    with open(config["log_file"], "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.now().isoformat()} - {message}\n")

def main() -> None:
    log(f"Starting update for {args.indicator}-based connections in {args.environment}.")

    # Extract configurations
    tableau_server_confs = config["tableau_server_info"]
    db_confs = config[f"{args.indicator}_connection_info"]

    # set up base url for REST API calls
    url = f"{tableau_server_confs["tableau_server"]}/api/{tableau_server_confs["api_version"]}"

    # sign in to Tableau Server via REST API and get auth token
    auth_token, site_id = sign_in(
        url = url,
        pat_name = tableau_server_confs["token_name"],
        pat_secret = tableau_server_confs["token_secret"],
        site = tableau_server_confs["site"]
    )

    # set up request headers
    headers = {
        "X-Tableau-Auth": auth_token,
        "Content-Type": "application/json"
    }

    # get required db connection details
    db_server_name = db_confs["db_server_name"]
    db_username = db_confs["db_username"]
    db_password = db_confs["db_password"]

    # iterate through each object type to update connections
    for obj_type in config["objects"]:

        if obj_type not in ["datasources", "flows", "workbooks"]:
            log(f"Skipping unknown object type: {obj_type}")
            continue

        log(f"Processing object type: {obj_type}...")
        all_objects = get_objects(url, site_id, headers, obj_type)

        for obj in all_objects:
            obj_id = obj.get("id")
            obj_name = obj.get("name")

            try:
                connections = get_connections(url, site_id, headers, obj_type, obj_id)
            except Exception as e:
                log(f"{obj_name} - ERROR fetching connections: {e}")
                continue
            for conn in connections:
                if conn.get("serverAddress") == db_server_name and conn.get("userName") == db_username:
                    
                    payload = {
                        "connection": {
                            "serverAddress": db_server_name,
                            "userName": db_username,
                            "password": db_password,
                            "embedPassword": "true"
                        }
                    }
                    
                    resp = update_connection(
                        url,
                        site_id,
                        headers,
                        obj_type,
                        obj_id,
                        conn.get("id"),
                        payload
                    )

                    status = "OK" if resp.status_code == 200 else f"NOK - ({resp.text})"
                    log(f"{obj_name} - ConnID: {conn.get("id")} - Status: {status} - Time: {datetime.now().isoformat()}")

    # sign out to close the session
    sign_out(
        url = url,
        auth_token = auth_token
    )

    log(f"Completed update for {args.indicator}-based connections in {args.environment}.")


if __name__ == "__main__":
    main()