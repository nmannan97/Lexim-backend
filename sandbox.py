from flask import Flask, jsonify
import pandas as pd

from flask_cors import CORS
from rds import LeximGPTRDS

import mysql.connector
from rds import *

from dotenv import load_dotenv
import os
import re

app = Flask(__name__)
CORS(app)

load_dotenv()

mydb = mysql.connector.connect(
    host="lexim-gpt-dev.czph03cyjldb.us-west-1.rds.amazonaws.com",
    port=3306,
    user="Naeem.Mannan",
    password="Welcome_2_Lexim",
    database="lexim_gpt" 
)

rds = LeximGPTRDS()

rds.set_rds_connection_str()

query1 = "SELECT * FROM aa_metrics;"
query2 = "SELECT * FROM aa_runhistory;"
query3 = "SELECT s3_link, first_name, last_name FROM user;"
query4 = 'SELECT org_guid, name FROM organization' 

success, df = rds.run_query_to_df(query1)
success, df1 = rds.run_query_to_df(query2)
success, df2 = rds.run_query_to_df(query3)
success, df3 = rds.run_query_to_df(query4)

name_lookup = dict(
    zip(
        df2["s3_link"].astype(str),
        df2["first_name"].astype(str) + " " + df2["last_name"].astype(str)
    )
)

rows = df[df['meta_data'].str.contains("2ba0ac41-b3aa-4b96-ba47-feeddc029ccd", na=False)]

@app.route("/", methods=["GET"])
def home():
    token_in = 0
    token_out = 0
    for item1, item2 in zip(rows['tokens_in'], rows['tokens_out']):
        token_in += item1
        token_out += item2
    data = {"inputTokens": f"{token_in:,}", "outputTokens": f"{token_out:,}", "inputCharge": f"{(token_in/1000)*1.15:,.2f}", "outputCharge": f"{(token_out/1000)*1.15:,.2f}"}
    return jsonify(data)


@app.route("/time-count", methods=["GET"])
def time_counter():
    time_compute = 0
    for item1, item2 in zip(rows['start_time'], rows['end_time']):
        if(item2 - item1) > 0:
            time_compute += item2 - item1

    data = {"timeComputeHours": f"{time_compute/60:,.2f}"}
    return jsonify(data)


@app.route("/users", methods=["GET"])
def users():
    output = {}
    target = "2ba0ac41-b3aa-4b96-ba47-feeddc029ccd"

    # Build user_lookup from df_user (s3_link contains org_guid)
    user_lookup = {}
    for _, row in df2.iterrows():
        s3_link = str(row.get("s3_link", ""))
        match = re.search(r"USERS/([a-f0-9\-]+)", s3_link)
        first_name = str(row.get("first_name", "")).strip()
        last_name = str(row.get("last_name", "")).strip()

        # Extract org_guid from s3_link
        parts = str(match).split('/')
        if "USERS" in parts:
            try:
                print(parts)
                org_index = parts.index("ORGANIZATION")
                org_guid = parts[org_index + 1]
                full_name = f"{first_name} {last_name}".strip()
                if org_guid and full_name:
                    user_lookup[org_guid] = full_name
            except (IndexError, ValueError):
                continue

    # Extract relevant columns from df_runhistory
    run_guids = df1["run_guid"].astype(str).tolist()
    org_guids = df1["org_guid"].astype(str).tolist()
    task_guids = df1["task_guid"].astype(str).tolist()

    # Group tasks by run_guid for matching org
    target_user = {}
    for run_guid, org_guid, task_guid in zip(run_guids, org_guids, task_guids):
        if org_guid == target:
            target_user.setdefault(run_guid, []).append(task_guid)

    # Process each run's tasks
    for run_id, task_list in target_user.items():
        try:
            org_guid = df1[df1['run_guid'] == run_id]['org_guid'].values[0]
        except IndexError:
            org_guid = None

        user_name = user_lookup.get(run_id, "Unknown")

        output[run_id] = {
            "tokens_in": 0,
            "tokens_out": 0,
            "duration_minutes": 0,
            "user_name": user_name,
            "tasks": {}
        }

        for task_id in task_list:
            row = df[df['task_guid'].astype(str) == str(task_id)]
            run_row = df1[df1["task_guid"] == str(task_id)]

            if row.empty:
                continue

            try:
                tokens_in = int(row['tokens_in'].values[0])
                tokens_out = int(row['tokens_out'].values[0])
            except Exception:
                tokens_in = tokens_out = 0

            try:
                raw_start = row['start_time'].values[0]
                raw_end = row['end_time'].values[0]
                start_time = pd.to_datetime(raw_start, errors='coerce')
                end_time = pd.to_datetime(raw_end, errors='coerce')
                if pd.isnull(start_time) or pd.isnull(end_time):
                    raise ValueError("start_time or end_time is NaT")
                duration_minutes = (end_time - start_time).total_seconds() / 60
            except Exception:
                duration_minutes = 0

            try:
                run_datetime_raw = run_row['run_date'].values[0] if 'run_date' in run_row.columns else None
                run_datetime = str(pd.to_datetime(run_datetime_raw, errors='coerce')) if run_datetime_raw else None
            except Exception:
                run_datetime = None

            # Accumulate
            output[run_id]["tokens_in"] += tokens_in
            output[run_id]["tokens_out"] += tokens_out
            output[run_id]["duration_minutes"] += duration_minutes

            # Task-level entry
            output[run_id]["tasks"][task_id] = {
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "duration": "{:,.2f} minutes".format(duration_minutes),
                "run_datetime": run_datetime
            }

    # Final formatting
    for user in output:
        minutes = output[user]["duration_minutes"]
        output[user]["duration"] = "{:,.2f} minutes".format(minutes)
        del output[user]["duration_minutes"]

    return jsonify(output)



if __name__ == "__main__":
    app.run(debug=True)