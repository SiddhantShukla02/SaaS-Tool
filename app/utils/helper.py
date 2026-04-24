import os
from google.oauth2.service_account import Credentials

def get_google_creds(scopes):
    info = {
        "type": os.environ["type"],
        "project_id": os.environ["project_id"],
        "private_key_id": os.environ["private_key_id"],
        "private_key": os.environ["private_key"].replace("\\n", "\n"),
        "client_email": os.environ["client_email"],
        "client_id": os.environ["client_id"],
        "auth_uri": os.environ["auth_uri"],
        "token_uri": os.environ["token_uri"],
        "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
        "client_x509_cert_url": os.environ["client_x509_cert_url"],
        "universe_domain": os.environ.get("universe_domain", "googleapis.com"),
    }
    return Credentials.from_service_account_info(info, scopes=scopes)



import gspread

def get_sheet_client(scopes):
    creds = get_google_creds(scopes)
    return gspread.authorize(creds)