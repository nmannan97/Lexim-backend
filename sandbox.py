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
target_org = '2ba0ac41-b3aa-4b96-ba47-feeddc029ccd'

query1 = "SELECT * FROM aa_metrics;"
query2 = "SELECT * FROM aa_runhistory;"
query3 = "SELECT s3_link, first_name, last_name FROM user;"
query4 = 'SELECT org_guid, name FROM organization' 
query5 = sql = """
WITH user_clean AS (
    SELECT 
        SUBSTRING_INDEX(SUBSTRING_INDEX(u.s3_link, 'USERS/', -1), '/', 1) AS extracted_user_guid,
        u.first_name,
        u.last_name,
        u.s3_link,
        o.name AS organization_name,
        o.org_guid
    FROM lexim_gpt.user u
    JOIN lexim_gpt.organization o 
        ON u.s3_link LIKE CONCAT('%%', o.org_guid, '%%')
),
runhistory_enriched AS (
    SELECT 
        rh.*,
        uc.first_name,
        uc.last_name,
        uc.organization_name
    FROM lexim_gpt.aa_runhistory rh
    LEFT JOIN user_clean uc
        ON rh.user_guid = uc.extracted_user_guid
)

SELECT 
    run_guid,
    task_guid,
    create_date,
    user_guid,
    first_name,
    last_name,
    organization_name,
    org_guid
FROM runhistory_enriched
WHERE org_guid = '{}';
""".format(target_org)

success, df = rds.run_query_to_df(query1)
success, df1 = rds.run_query_to_df(query2)
success, df2 = rds.run_query_to_df(query3)
success, df3 = rds.run_query_to_df(query4)
success, df4 = rds.run_query_to_df(query5)

rows = df[df['meta_data'].str.contains(target_org, na=False)]

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
  
    # Build user_guid → "First Last" name mapping
    name_lookup = {
        str(row["user_guid"]): f"{row['first_name']} {row['last_name']}".strip()
        for _, row in df4.iterrows()
    }

    # Group task_guids by run_guid for the target org
    target_user = {}
    for _, row in df1.iterrows():
        if str(row["org_guid"]) == target_org:
            run_guid = str(row["run_guid"])
            task_guid = str(row["task_guid"])
            target_user.setdefault(run_guid, []).append(task_guid)

    # Build usage stats per run
    for run_id, task_list in target_user.items():
        output[run_id] = {
            "tokens_in": 0,
            "tokens_out": 0,
            "duration_minutes": 0,
            "tasks": {}
        }

        for task_id in task_list:
            row = df[df['task_guid'].astype(str) == task_id]
            run_row = df1[df1["task_guid"].astype(str) == task_id]

            if row.empty or run_row.empty:
                continue

            # Parse token counts
            try:
                tokens_in = int(row['tokens_in'].values[0])
                tokens_out = int(row['tokens_out'].values[0])
            except Exception:
                tokens_in = tokens_out = 0

            # Parse compute time
            try:
                start_time = pd.to_datetime(row['start_time'].values[0], errors='coerce')
                end_time = pd.to_datetime(row['end_time'].values[0], errors='coerce')
                duration_minutes = (end_time - start_time).total_seconds() / 60 if start_time and end_time else 0
            except Exception:
                duration_minutes = 0

            # Parse run datetime
            try:
                run_datetime = pd.to_datetime(run_row['create_date'].values[0], errors='coerce')
                run_datetime = str(run_datetime) if pd.notnull(run_datetime) else None
            except Exception:
                run_datetime = None

            # Get user name from user_guid
            try:
                user_guid = str(run_row['user_guid'].values[0])
            except Exception:
                user_guid = "Unknown"

            user_name = name_lookup.get(user_guid, "Unknown")

            # Accumulate task info
            output[run_id]["tokens_in"] += tokens_in
            output[run_id]["tokens_out"] += tokens_out
            output[run_id]["duration_minutes"] += duration_minutes

            output[run_id]["tasks"][task_id] = {
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "duration": f"{duration_minutes:,.2f} minutes",
                "run_datetime": run_datetime,
                "user_name": user_name
            }

        # Format run-level duration
        output[run_id]["duration"] = f"{output[run_id]['duration_minutes']:,.2f} minutes"
        del output[run_id]["duration_minutes"]

    return jsonify(output)


if __name__ == "__main__":
    app.run(debug=True)