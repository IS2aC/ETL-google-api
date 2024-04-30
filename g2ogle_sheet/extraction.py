import os.path
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# scopes for 
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def extract_data_from_sheet(spreadsheet_id:str, sheet_name:str, scopes:list = SCOPES):
    """
    Extract data from google sheet

    Args:
        spreadsheet_id (str): id of google sheet page
        sheet_name (str): sheet name 
        scopes (list, optional): scopes for api call for extraction. Defaults to SCOPES.

    Returns:
        _type_: _description_
    """

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", scopes
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A:Z")
            .execute()
        )
        values = result.get("values", [])

        if not values:
            print("No data found.")
            return

        # print("Name, Major:")
        list_row = []

        for row in values:
            list_row.append(row)

        # Création du DataFrame
        df = pd.DataFrame(list_row[1:], columns=list_row[0])

        return df
    except HttpError as err:
        print(err)