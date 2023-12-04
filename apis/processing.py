import zipfile
import json
import os
import pandas as pd
import psycopg2
import os
import pandas as pd
from flask import Flask, request, jsonify
from fastapi import FastAPI, UploadFile, Request

app = FastAPI()

@app.post("/create_database")
async def create_database(file: UploadFile):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="kentron",
        user="postgres",
        password="Kingfr@ncesco015"
    )

    # Specify the directory to extract the zip file to
    extract_directory = '../data4/raw/'

    # Save the zip file
    zip_file_path = os.path.join(extract_directory, file.filename)
    with open(zip_file_path, 'wb') as f:
        f.write(await file.read())

    # Extract the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_directory)

    # Get a list of JSON files in the directory
    json_files = [os.path.join(root, f) for root, _, files in os.walk(extract_directory) for f in files if f.endswith('.json')]

    # Process each JSON file and convert it to a Pandas DataFrame
    df_list = []
    for json_file in json_files:
        with open(json_file, encoding='utf-8') as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        df_list.append(df)

        # Convert JSON file to CSV
        csv_file = os.path.splitext(json_file)[0] + '.csv'
        print(csv_file)
        df.to_csv(csv_file, index=False)

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(df_list, ignore_index=True)

    # Create tables and insert data into the PostgreSQL database
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed_files (id SERIAL PRIMARY KEY, file_name VARCHAR, file_data JSONB)")
    cursor.execute("CREATE TABLE IF NOT EXISTS user_mapping (user_id VARCHAR PRIMARY KEY, username VARCHAR)")
    cursor.execute("CREATE TABLE IF NOT EXISTS channel_info (channel_name VARCHAR, members VARCHAR, type VARCHAR)")
    cursor.execute("CREATE TABLE IF NOT EXISTS single_user_messages (user_id VARCHAR, message VARCHAR)")
    cursor.execute("CREATE TABLE IF NOT EXISTS two_user_messages (user_id VARCHAR, message VARCHAR)")

    # for index, row in df.iterrows():
    #     file_name = os.path.basename(row['id'])
    #     file_data = row.to_json()
    #     cursor.execute("INSERT INTO processed_files (file_name, file_data) VALUES (%s, %s)", (file_name, file_data))

    csv_file_path = '../csv/users.csv'
    df = pd.read_csv(csv_file_path)
    user_mapping = df.set_index('id')['real_name'].to_dict()
    for user_id, username in user_mapping.items():
        cursor.execute("INSERT INTO user_mapping (user_id, username) VALUES (%s, %s)", (user_id, username))

    csv_file_path = '../csv/channels.csv'
    df = pd.read_csv(csv_file_path)
    channel_info_df = pd.DataFrame(columns=['channel_name', 'members', 'type'])
    for index, row in df.iterrows():
        # print(row)
        channel_name = row['name']
        members = row['members']
        channel_type = row['created']
        members_list = members.split(',')
        for member in members_list:
            channel_info_df = channel_info_df.append({'channel_name': channel_name, 'members': member, 'type': channel_type}, ignore_index=True)
            cursor.execute("INSERT INTO channel_info (channel_name, members, type) VALUES (%s, %s, %s)", (channel_name, member, channel_type))

    csv_file_path = '../notebooks/merged_msg.csv'
    df = pd.read_csv(csv_file_path)
    for index, row in df.iterrows():
        user_id = row['user']
        message = row['text']
        cursor.execute("INSERT INTO single_user_messages (user_id, message) VALUES (%s, %s)", (user_id, message))

    # filtered_df = df[(df['user_id'] == user_id1) | (df['user_id'] == user_id2)]
    # for index, row in filtered_df.iterrows():
    #     user_id = row['user_id']
    #     message = row['message']
    #     cursor.execute("INSERT INTO two_user_messages (user_id, message) VALUES (%s, %s)", (user_id, message))

    conn.commit()
    cursor.close()

    # Retrieve the list of channels with channel type and constituent members with user name
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM channel_info")
    #type, array_agg(username) FROM channel_info JOIN user_mapping ON channel_info.members = user_mapping.user_id GROUP BY channel_name")
    channel_data = cursor.fetchall()
    cursor.close()
    conn.close()

    return {"channels": channel_data}


@app.get("/get_user_messages/{username}")
def get_messages(username: str):
    user_id_to_username = {
    "U05RA88PCPR": "sam",
    "U05S28VB3ED": "admin",
    "U05S28X7ETB": "payagude.m",
    "U05TG4W1YUF": "nikhil993477",
    "U05TQLV3ZC4": "satish",
    "U05TTNUB27P": "sajals1146",
    "U05TZ4E0G5A": "shubhangisharma2411",
    "U05V4DFEHS8": "tanushsethi55"
    }
    # Read the merged_msg.csv file
    merged_data = pd.read_csv('../notebooks/merged_msg.csv')

    # Replace user_id with username in user column
    merged_data['user'] = merged_data['user'].replace(user_id_to_username)

    # Filter the data based on the username
    filtered_data = merged_data[merged_data['user'] == username]

    # Convert the filtered messages to JSON
    messages_json = filtered_data.to_json(orient='records')

    return messages_json


@app.get("/get_user_messages_between")
async def get_user_messages_between(username1: str, username2: str):
    # Read the dms_output.csv file
    dms_data = pd.read_csv('../processed_data/dms_output.csv')

    # Filter the data based on the usernames
    filtered_data = dms_data[(dms_data['member_1'] == username1) & (dms_data['member_2'] == username2)]
    if filtered_data.empty:
        return {'message': 'No matching records found'}

    # Get the folder name from the id column of the matched record
    folder_name = filtered_data.iloc[0]['id']

    # Search for the folder with the same name
    folder_path = None
    for root, dirs, files in os.walk('../data4/raw'):
        if folder_name in dirs:
            folder_path = os.path.join(root, folder_name)
            break

    if folder_path is None:
        return {'message': 'Folder not found'}

    # Merge all the CSV files in the folder
    merged_data = pd.DataFrame()
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path, usecols=["user", "text", "type", "ts"])
            df['ts'] = pd.to_datetime(df['ts'], unit='s')  # Convert ts column to human-readable timestamp
            merged_data = pd.concat([merged_data, df])

    # Filter the merged data based on the usernames
    user_id_to_username = {
    "U05RA88PCPR": "sam",
    "U05S28VB3ED": "admin",
    "U05S28X7ETB": "payagude.m",
    "U05TG4W1YUF": "nikhil993477",
    "U05TQLV3ZC4": "satish",
    "U05TTNUB27P": "sajals1146",
    "U05TZ4E0G5A": "shubhangisharma2411",
    "U05V4DFEHS8": "tanushsethi55"
    }
    merged_data['user'] = merged_data['user'].replace(user_id_to_username)
    filtered_messages = merged_data[(merged_data['user'] == username1) | (merged_data['user'] == username2)]


    # Convert the filtered messages to JSON
    messages_json = filtered_messages.to_json(orient='records')

    return messages_json

