from flask import Flask, jsonify
import pandas as pd

from flask_cors import CORS

app = Flask(__name__)
CORS(app)

df = pd.read_csv("metrics.csv", encoding="cp1252")
df1 = pd.read_csv("runhistory.csv", encoding="cp1252")

rows = df[df['meta_data'].str.contains("2ba0ac41-b3aa-4b96-ba47-feeddc029ccd", na=False)]

df2 = df1.iloc[[2, 7, 8]]

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

    print("time computed = {:,.2f}".format(time_compute/60))
    data = {"timeComputeHours": f"{time_compute/60:,.2f}"}
    return jsonify(data)

@app.route("/users", methods=["GET"])
def users():
    output = {}
    target = "2ba0ac41-b3aa-4b96-ba47-feeddc029ccd"

    user_guids = df1["user_guid"].astype(str).tolist()
    org_guids = df1["org_guid"].astype(str).tolist()
    task_guids = df1["task_guid"].astype(str).tolist()
    
    target_user = {}

    # Group tasks by user under matching org
    for user_guid, org_guid, task_guid in zip(user_guids, org_guids, task_guids):
        if org_guid == target:
            target_user.setdefault(user_guid, []).append(task_guid)

    # Process each user's tasks
    for user_id, task_list in target_user.items():
        output[user_id] = {
            "tokens_in": 0,
            "tokens_out": 0,
            "duration_minutes": 0,
            "tasks": {}
        }

        for task_id in task_list:
            row = df[df['task_guid'].astype(str) == str(task_id)]
            run_row = df1[df1['task_guid'].astype(str) == str(task_id)]

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
            except Exception as e:
                print(f"⚠️ Time error for {task_id}: {e}")
                duration_minutes = 0

            # Safely extract run_datetime
            try:
                raw_run_time = run_row['run_datetime'].values[0]
                run_datetime = str(pd.to_datetime(raw_run_time, errors='coerce'))
            except:
                run_datetime = None

            # Accumulate user totals
            output[user_id]["tokens_in"] += tokens_in
            output[user_id]["tokens_out"] += tokens_out
            output[user_id]["duration_minutes"] += duration_minutes

            # Add per-task data
            output[user_id]["tasks"][task_id] = {
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "duration": "{:,.2f} minutes".format(duration_minutes),
                "run_datetime": run_datetime
            }

    # Final formatting of total duration
    for user in output:
        minutes = output[user]["duration_minutes"]
        output[user]["duration"] = "{:,.2f} minutes".format(minutes)
        del output[user]["duration_minutes"]

    return jsonify(output)


if __name__ == "__main__":
    app.run(debug=True)