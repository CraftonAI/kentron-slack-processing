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

    return {"message": "Files extracted and converted successfully."}
