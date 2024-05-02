import os
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload


SCOPES  = ["https://www.googleapis.com/auth/drive"]


def authenticate_drive():
    """ Authenticate function to drive ! """
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds


def create_drive_repertory_if_exists(drive_repertory_name, creds):

    """Call Google's API for check if the folder exists on the drive space."""
    service = build("drive", "v3", credentials=creds)

    response = service.files().list(
        q="name='" + drive_repertory_name + "' and mimeType='application/vnd.google-apps.folder'",
        spaces='drive'
    ).execute()

    if not response['files']:  # Si le dossier n'existe pas
        file_metadata = {
            "name": drive_repertory_name,
            "mimeType": "application/vnd.google-apps.folder"
        }

        # creation of the folder
        file = service.files().create(
            body=file_metadata,
            fields="id"
        ).execute()

        folder_id = file.get('id')
    else:  # Si le dossier existe déjà
        folder_id = response['files'][0]['id']
    return folder_id

def upload_gdrive(local_file_path, drive_folder_name):
    """Function to upload file on google drive space """
    creds = authenticate_drive()
    folder_id = create_drive_repertory_if_exists(drive_folder_name, creds)

    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": os.path.basename(local_file_path),
        "parents": [folder_id]
    }

    media = MediaFileUpload(local_file_path, resumable=True)

    upload_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print(f"Le fichier {os.path.basename(local_file_path)} a été téléchargé avec succès dans le dossier {drive_folder_name} sur Google Drive.")


if __name__ == "__main__":
    upload_gdrive(local_file_path =  'backupfiles/data_test_file.csv', drive_folder_name =  'BackupFolder2022')


