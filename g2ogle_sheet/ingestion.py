from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import pandas as pd

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def ingestion_data_on_sheet(dataframe: pd.DataFrame, spreadsheet_id:str, value_input_option:str, range_name = None, scopes = SCOPES) -> None:
    creds = None
    
    # check if token path exists on the file system
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", scopes)
    # check unit for api call to google
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", scopes
            )
            creds = flow.run_local_server(port=0)
        # load information 
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    values =  dataframe.values.tolist()
    service = build("sheets", "v4", credentials=creds)

    try:
        if range_name is not None:
            body = {"values": values}
            result = (
                service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption=value_input_option,
                    body=body,
                )
                .execute()
            )
            print(f"{result.get('updatedCells')} cells updated.")
        else:
            body = {"values": values}
            result = (
                service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=spreadsheet_id,
                    valueInputOption=value_input_option,
                    
                    # for dataframe for more than 26 variables we will have some problems of data alterations
                    range="A:Z",
                    body=body,
                )
                .execute()
            )
            print("row added successfully")
    except HttpError as error:
        print(f"An error occurred: {error}")

