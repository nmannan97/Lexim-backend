from flask import Flask, request, jsonify
import pandas as pd

from flask_cors import CORS
from rds import LeximGPTRDS

import mysql.connector
from rds import *

from dotenv import load_dotenv
from datetime import datetime

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
query_agent = f"""
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
    ON u.s3_link LIKE CONCAT('%', o.org_guid, '%')
),
runhistory_enriched AS (
  SELECT 
    rh.run_guid,
    rh.task_guid,
    rh.agent_code,
    rh.run_datetime,
    rh.create_date,
    rh.user_guid,
    rh.org_guid,
    uc.first_name,
    uc.last_name,
    uc.organization_name
  FROM lexim_gpt.aa_runhistory rh
  LEFT JOIN user_clean uc
    ON rh.user_guid = uc.extracted_user_guid
),
runhistory_with_agents AS (
  SELECT 
    re.*,
    aa.display_name AS agent_display_name,
    m.tokens_in,
    m.tokens_out,
    m.start_time,
    m.end_time
  FROM runhistory_enriched re
  LEFT JOIN lexim_gpt.aa_agent aa 
    ON re.agent_code = aa.code
  LEFT JOIN lexim_gpt.aa_metrics m 
    ON re.task_guid = m.task_guid
)
SELECT *
FROM runhistory_with_agents
WHERE org_guid = '{target_org}';
"""

success, df = rds.run_query_to_df(query1)
success, df1 = rds.run_query_to_df(query2)
success, df2 = rds.run_query_to_df(query3)
success, df3 = rds.run_query_to_df(query4)
success, df4 = rds.run_query_to_df(query_agent)

success, df_agent = rds.run_query_to_df(query_agent)

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

    # task_guid → agent_display_name
    agent_lookup = {
        str(row["task_guid"]): row["agent_display_name"]
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
            "task_name": "",
            "tokens_in": 0,
            "tokens_out": 0,
            "duration_minutes": 0,
            "run_datetime": None,
            "user_name": ""
        }

        for task_id in task_list:
            row = df[df['task_guid'].astype(str) == task_id]
            run_row = df1[df1["task_guid"].astype(str) == task_id]

            if row.empty or run_row.empty:
                #print(f"No data found for task_id: {task_id}")
                continue

            # Parse token counts
            tokens_in = int(row['tokens_in'].values[0]) if 'tokens_in' in row else 0
            tokens_out = int(row['tokens_out'].values[0]) if 'tokens_out' in row else 0

            # Parse compute time
            try:
                start_time = float(row['start_time'].values[0])
                end_time = float(row['end_time'].values[0])
                duration = (end_time - start_time) / 60 if start_time and end_time else 0
            except Exception as e:
                print("Exception occurred as:", e)
                duration_minutes = 0

            # Get task name
            task_name = agent_lookup.get(task_id, "")

            # Run datetime
            run_datetime = pd.to_datetime(run_row['create_date'].values[0], errors='coerce')
            run_datetime = str(run_datetime) if pd.notnull(run_datetime) else None

            # User name
            user_guid = str(run_row['user_guid'].values[0])
            user_name = name_lookup.get(user_guid, "Unknown")
            
            #print(f"[{task_id}] start_time: {row['start_time'].values}, end_time: {row['end_time'].values}")

            # Aggregate
            output[run_id]["tokens_in"] += tokens_in
            output[run_id]["tokens_out"] += tokens_out
            output[run_id]["duration_minutes"] += duration
            output[run_id]["task_name"] = task_name
            output[run_id]["run_datetime"] = run_datetime
            output[run_id]["user_name"] = user_name

        # Format duration nicely
        output[run_id]["duration_minutes"] = f"{output[run_id]['duration_minutes']:,.2f} minutes"

    return jsonify(output)


@app.route("/daily-usage", methods=["GET"])
def daily_usage():
    from datetime import datetime, timedelta
    today = datetime.now().date()
    seven_days_ago = today - timedelta(days=6)

    # Initialize daily buckets
    usage_summary = {}

    for i in range(7):
        day = (today - timedelta(days=i)).isoformat()
        usage_summary[day] = {
            "cpu_hours": 0.0,
            "storage_gb": 0.0,
            "tokens_used": 0,
            "total_cost": 0.0
        }

    for _, row in df1.iterrows():
        run_datetime = pd.to_datetime(row.get("create_date"), errors="coerce")
        if pd.isnull(run_datetime):
            continue

        run_date = run_datetime.date().isoformat()
        if run_date not in usage_summary:
            continue  # skip if outside the 7-day window

        # CPU time in minutes → convert to hours
        try:
            start = float(row.get("start_time", 0))
            end = float(row.get("end_time", 0))
            cpu_minutes = max(0, (end - start) / 60)
        except:
            cpu_minutes = 0

        # Tokens used
        task_id = str(row.get("task_guid"))
        df_task = df[df['task_guid'].astype(str) == task_id]
        if df_task.empty:
            continue

        tokens_in = int(df_task["tokens_in"].values[0]) if "tokens_in" in df_task and not df_task.empty else 0
        tokens_out = int(df_task["tokens_out"].values[0]) if "tokens_out" in df_task and not df_task.empty else 0

        tokens_used = tokens_in + tokens_out

        # Simulated costs and storage (adjust as needed)
        cost_per_token = 0.0001
        cost_per_cpu_hour = 0.01
        storage_per_task_gb = 0.05  # placeholder per task

        usage_summary[run_date]["cpu_hours"] += cpu_minutes / 60
        usage_summary[run_date]["storage_gb"] += storage_per_task_gb
        usage_summary[run_date]["tokens_used"] += tokens_used
        usage_summary[run_date]["total_cost"] += (
            tokens_used * cost_per_token + (cpu_minutes / 60) * cost_per_cpu_hour
        )

    # Format output nicely
    for day in usage_summary:
        usage_summary[day]["cpu_hours"] = round(usage_summary[day]["cpu_hours"], 2)
        usage_summary[day]["storage_gb"] = round(usage_summary[day]["storage_gb"], 2)
        usage_summary[day]["total_cost"] = round(usage_summary[day]["total_cost"], 4)

    return jsonify(usage_summary)


@app.route("/task-history", methods=["GET"])
def task_history():
    if not success or df4.empty:
        return jsonify({"error": "Failed to fetch data"}), 500

    # Extract filters from query string
    user_filter = request.args.get("user")
    task_filter = request.args.get("task")
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    sort_by = request.args.get("sort", "create_date")
    sort_order = request.args.get("order", "desc")

    # Process records
    records = []
    for _, row in df4.iterrows():
        run_dt = pd.to_datetime(row.get("create_date"), errors="coerce")
        if pd.isnull(run_dt):
            continue

        tokens_in_raw = row.get("tokens_in", 0)
        tokens_out_raw = row.get("tokens_out", 0)
        cpu_minutes_raw = row.get("cpu_time_minutes", 0)

        tokens_in = 0 if pd.isna(tokens_in_raw) else int(tokens_in_raw)
        tokens_out = 0 if pd.isna(tokens_out_raw) else int(tokens_out_raw)
        cpu_minutes = 0.0 if pd.isna(cpu_minutes_raw) else float(cpu_minutes_raw)

        record = {
            "task_name": row.get("agent_display_name", "Unknown"),
            "execution_time": run_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "user_name": f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cpu_minutes": cpu_minutes,
            "estimated_cost": "{:,.2f}".format(round((tokens_in + tokens_out) / 1000 * 1.15, 2))
        }

        # Apply filters
        if user_filter and user_filter.lower() not in record["user_name"].lower():
            continue
        if task_filter and task_filter.lower() not in record["task_name"].lower():
            continue
        if start_date and run_dt.date() < pd.to_datetime(start_date).date():
            continue
        if end_date and run_dt.date() > pd.to_datetime(end_date).date():
            continue

        records.append(record)

    # Sort results
    records.sort(key=lambda x: x.get(sort_by, ""), reverse=(sort_order == "desc"))

    return jsonify(records)


@app.route('/agent-usage', methods=['GET'])
def agent_usage():
    if not success or df_agent.empty:
        return jsonify({"error": "No data available"}), 404

    # Optional filters
    agent_filter = request.args.get('agent')
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    df = df_agent.copy()

    # Parse datetime
    df['create_date'] = pd.to_datetime(df['create_date'], errors='coerce')
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')

    # Compute duration
    df['cpu_minutes'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60
    df['cpu_hours'] = df['cpu_minutes'] / 60

    # Clean tokens
    df['tokens_in'] = pd.to_numeric(df['tokens_in'], errors='coerce').fillna(0)
    df['tokens_out'] = pd.to_numeric(df['tokens_out'], errors='coerce').fillna(0)

    # Apply filters
    if agent_filter:
        df = df[df['agent_display_name'].str.contains(agent_filter, case=False, na=False)]
    if start_date:
        df = df[df['create_date'] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df['create_date'] <= pd.to_datetime(end_date)]

    # Group by agent
    summary = df.groupby('agent_display_name').agg(
        runs=('run_guid', 'count'),
        total_tokens_in=('tokens_in', 'sum'),
        total_tokens_out=('tokens_out', 'sum'),
        total_cpu_minutes=('cpu_minutes', 'sum'),
        total_cpu_hours=('cpu_hours', 'sum')
    ).reset_index()

    summary['estimated_cost'] = ((summary['total_tokens_in'] + summary['total_tokens_out']) / 1000 * 1.15).round(2)

    return jsonify(summary.to_dict(orient='records'))


if __name__ == "__main__":
    app.run(debug=True)