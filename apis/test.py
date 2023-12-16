from fastapi import FastAPI, UploadFile
import os
import zipfile
import json
import pandas as pd

app = FastAPI()

@app.post("/extract_and_convert")
async def extract_and_convert(file: UploadFile):
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
        df.to_csv(csv_file, index=False)

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(df_list, ignore_index=True)

    # Read the channels.csv file
    channels_df = pd.read_csv('../data4/raw/channels.csv')

    # Select the desired columns
    channel_info_df = channels_df[['id', 'name', 'is_general', 'creator', 'created', 'members', 'is_archived']]

    # Specify the directory to save the channel_info.csv file
    save_directory = '../data4/processed/'

    # Save the channel_info DataFrame to a CSV file
    channel_info_df.to_csv(os.path.join(save_directory, 'channel_info.csv'), index=False)

    # Read the users.csv file
    users_df = pd.read_csv('../data4/raw/users.csv')

    # Select the desired columns
    users_info_df = users_df[['id', 'team_id', 'name', 'deleted', 'is_bot', 'is_app_user', 'updated']]

    # Save the users_info DataFrame to a CSV file
    users_info_df.to_csv(os.path.join(save_directory, 'users_info.csv'), index=False)

    # Create a dictionary to map id and name
    id_name_mapping = dict(zip(users_info_df['id'], users_info_df['name']))

    # Define the directory path
    directory_path = "../data4/raw/"

    # Define the column names
    column_names = ["user", "text", "type", "ts"]

    # Create an empty dataframe
    df = pd.DataFrame(columns=column_names)

    # Loop through all subdirectories starting with "D05"
    for subdir, dirs, files in os.walk(directory_path):
        if subdir.startswith(directory_path + "D05"):
            for file in files:
                if file.endswith(".csv"):
                    # Read the csv file
                    file_path = os.path.join(subdir, file)
                    temp_df = pd.read_csv(file_path)
                    
                    # Check if all required columns are present
                    if all(col in temp_df.columns for col in column_names):
                        # Append the dataframe to the main dataframe
                        df = pd.concat([df, temp_df[column_names]], ignore_index=True)

    df['ts'] = pd.to_datetime(df['ts'], unit='s')

    # Save the dataframe to a csv file
    df.to_csv("../data4/processed/merged_msg.csv", index=False)


    dms_df = pd.read_csv('../data4/raw/dms.csv')

    # Divide each user in the 'members' column into different columns
    members_df = dms_df['members'].str.split(',', expand=True)
    members_df = members_df.replace({',':'', '\[':'', '\]':'', '\'':''}, regex=True)

    # Rename the columns
    members_df.columns = [f"member_{i+1}" for i in range(members_df.shape[1])]
    members_df['member_2'] = members_df['member_2'].str.replace(' ', '')
    members_df = members_df.replace(id_name_mapping)

    # Concatenate the original DataFrame with the new columns
    dms_df = pd.concat([dms_df, members_df], axis=1)

    # Display the modified DataFrame
    dms_df.to_csv('../data4/processed/dms_output.csv', index=False)

    return {"message": "Files extracted and converted successfully.", "id_name_mapping": id_name_mapping}

@app.get("/get_user_messages/{username}")
def get_messages(username: str):

    # Read the merged_msg.csv file
    merged_data = pd.read_csv('../data4/processed/merged_msg.csv')

    # Replace user_id with username in user column
    merged_data['user'] = merged_data['user'].replace(id_name_mapping)

    # Filter the data based on the username
    filtered_data = merged_data[merged_data['user'] == username]

    # Convert the filtered messages to JSON
    messages_json = filtered_data.to_json(orient='records')

    return messages_json


@app.get("/get_user_messages_between")
async def get_user_messages_between(username1: str, username2: str):
    # Read the dms_output.csv file
    dms_data = pd.read_csv('../data4/processed/dms_output.csv')

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
    merged_data['user'] = merged_data['user'].replace(id_name_mapping)
    filtered_messages = merged_data[(merged_data['user'] == username1) | (merged_data['user'] == username2)]

    # Convert the filtered messages to JSON
    messages_json = filtered_messages.to_json(orient='records')

    return messages_json
