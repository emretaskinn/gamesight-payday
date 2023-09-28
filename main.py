import requests
import os
import datetime as dt
import pandas as pd
import json
from google.cloud import storage
from flask import Flask, Response

pd.set_option('display.expand_frame_repr', False)

app = Flask(__name__)


@app.route("/")
def get_gamesight_data():
    ## GCS upload functions
    def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
        """Uploads a file to the bucket."""

        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"

        # The contents to upload to the file
        # contents = "these are my contents"

        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(contents, "text/csv")

        print(
            f"{destination_blob_name} with contents {contents} uploaded to {bucket_name}."
        )

    for d in range(11):
        url = "https://api.marketing.gamesight.io/stats"

        day = dt.date.today() - dt.timedelta(days=d+1)
        day = dt.date.isoformat(day)

        payload = {
            "filters": {
                "include_unattributed": False,
                "team_ids": [5652279, 5652203],
                "start_triggered_at_date": day,
                "end_triggered_at_date": day
            },
            "timezone": "UTC",
            "game_id": 1219,
            "fields": ['goals'],
            "groups": ["game_id", "team_id", "network", "campaign", "triggered_at_date", "country", "platform"]
        }
        headers = {
            "accept": "application/json",
            "Authorization": "7e0ee6511ea8f08932e71a5910e936ef",
            "X-Api-Version": "3.0.0",
            "content-type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)

        print(response.text)
        # writing the data in json format
        json_results = response.json()
        #len(json_results['results'])

        # turning json into dataframe
        if len(json_results['results']) > 0:
            print('results found')
            df = pd.DataFrame(json_results['results'])

            df['Install'] = 0
            goals = df['goals'].to_json()
            parsed = json.loads(goals)
            parsed = {int(k): v for k, v in
                      parsed.items()}  # json key objects comes as string. this code changes them to int

            for i in parsed:
                if '2190' in parsed[i].keys() and 'standard' in parsed[i]['2190'].keys():
                    df.at[i, 'Install'] = parsed[i]['2190']['standard']['count']
                elif '2190' not in parsed[i].keys():
                    df.at[i,'Install'] = 0
                else:
                    df.at[i,'Install'] = 0


            filename = 'payday-gs-marketing_{}.csv'.format(day)
            upload_blob_from_memory('payday-gamesight-reporting-bucket', df.to_csv(index=False),filename)


    return Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename="+filename})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))