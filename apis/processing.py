import zipfile
import json
import os
import pandas as pd
import psycopg2
from fastapi import FastAPI, UploadFile

app = FastAPI()

@app.post("/create_database")
async def create_database(file: UploadFile):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="your_database_name",
        user="your_username",
        password="your_password"
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

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(df_list, ignore_index=True)

    # Create tables and insert data into the PostgreSQL database
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed_files (id SERIAL PRIMARY KEY, file_name VARCHAR, file_data JSONB)")
    cursor.execute("CREATE TABLE IF NOT EXISTS user_mapping (user_id VARCHAR PRIMARY KEY, username VARCHAR)")
    cursor.execute("CREATE TABLE IF NOT EXISTS channel_info (channel_name VARCHAR, members VARCHAR, type VARCHAR)")
    cursor.execute("CREATE TABLE IF NOT EXISTS single_user_messages (user_id VARCHAR, message VARCHAR)")
    cursor.execute("CREATE TABLE IF NOT EXISTS two_user_messages (user_id VARCHAR, message VARCHAR)")

    for index, row in df.iterrows():
        file_name = row['file_name']
        file_data = row.to_json()
        cursor.execute("INSERT INTO processed_files (file_name, file_data) VALUES (%s, %s)", (file_name, file_data))

    csv_file_path = '../csv/users.csv'
    df = pd.read_csv(csv_file_path)
    user_mapping = df.set_index('user_id')['username'].to_dict()
    for user_id, username in user_mapping.items():
        cursor.execute("INSERT INTO user_mapping (user_id, username) VALUES (%s, %s)", (user_id, username))

    csv_file_path = '../csv/channels.csv'
    df = pd.read_csv(csv_file_path)
    channel_info_df = pd.DataFrame(columns=['channel_name', 'members', 'type'])
    for index, row in df.iterrows():
        channel_name = row['channel_name']
        members = row['members']
        channel_type = row['type']
        members_list = members.split(',')
        for member in members_list:
            channel_info_df = channel_info_df.append({'channel_name': channel_name, 'members': member, 'type': channel_type}, ignore_index=True)
            cursor.execute("INSERT INTO channel_info (channel_name, members, type) VALUES (%s, %s, %s)", (channel_name, member, channel_type))

    csv_file_path = '../csv/messages.csv'
    df = pd.read_csv(csv_file_path)
    filtered_df = df[df['user_id'] == user_id]
    for index, row in filtered_df.iterrows():
        user_id = row['user_id']
        message = row['message']
        cursor.execute("INSERT INTO single_user_messages (user_id, message) VALUES (%s, %s)", (user_id, message))

    filtered_df = df[(df['user_id'] == user_id1) | (df['user_id'] == user_id2)]
    for index, row in filtered_df.iterrows():
        user_id = row['user_id']
        message = row['message']
        cursor.execute("INSERT INTO two_user_messages (user_id, message) VALUES (%s, %s)", (user_id, message))

    conn.commit()
    cursor.close()

    # Retrieve the list of channels with channel type and constituent members with user name
    cursor = conn.cursor()
    cursor.execute("SELECT channel_name, type, array_agg(username) FROM channel_info JOIN user_mapping ON channel_info.members = user_mapping.user_id GROUP BY channel_name, type")
    channel_data = cursor.fetchall()
    cursor.close()
    conn.close()

    return {"channels": channel_data}
