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
    print("✅ /users route hit")
    
    output = {}

    target = "2ba0ac41-b3aa-4b96-ba47-feeddc029ccd"

    user_guids = df1["user_guid"].tolist()
    org_guids = df1["org_guid"].tolist()
    task_guids = df1["task_guid"].tolist()
    target_user = {}

    for item1, item2, item3 in zip(user_guids, org_guids, task_guids):
        try:
            if item2 == target:
                try:
                    target_user[item1].append(item3)
                except:
                    target_user[item1] = []
        except Exception as e:
            print(f"⚠️ Error processing row: {e}")
            continue

    for item in target_user:
        for prop in target_user[item]:
            row = df[df['task_guid'].astype(str) == str(prop)]

            if row.empty:
                print(f"⚠️ No match found for task_guid: {prop}")
                continue

            try:
                tokens_in = int(row['tokens_in'].values[0])
                tokens_out = int(row['tokens_out'].values[0])
            except Exception as e:
                print(f"⚠️ Token error for {prop}: {e}")
                tokens_in, tokens_out = 0, 0

            try:
                start_time = pd.to_datetime(row['start_time'].values[0], errors='coerce')
                end_time = pd.to_datetime(row['end_time'].values[0], errors='coerce')

                if pd.isnull(start_time) or pd.isnull(end_time):
                    raise ValueError("start_time or end_time is NaT")

                duration_seconds = (end_time - start_time).total_seconds()
                duration = "{:,.2f} minutes".format(duration_seconds / 60)
            except Exception as e:
                print(f"⚠️ Time error for {prop}: {e}")
                duration = "0.00 minutes"

            output[prop] = [tokens_in, tokens_out, duration]

        print(output)
    return jsonify(output)

if __name__ == "__main__":
    app.run(debug=True)