import msal
import requests
import base64
import os
import io
import configparser
import pandas as pd
import tempfile
import subprocess
import sys
import pyodbc

print(os.getcwd())
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(BASE_DIR, "config.cfg")
print(f"Loading configuration from: {config_path}")

config = configparser.ConfigParser()
config.read(config_path)
az = config['azure']

CLIENT_ID = az['CLIENT_ID']
CLIENT_SECRET = az['CLIENT_SECRET']
TENANT_ID = az['TENANT_ID']
MAILBOX = az['MAILBOX']
# DOWNLOAD_DIR = 'downloads'
# OUTPUT_DIR = 'outputs'

GRAPH_ENDPOINT = 'https://graph.microsoft.com'
SQL_SERVER     = 'gincorsql01.database.windows.net'
SQL_DATABASE   = 'dashboard'

LOCATION_TABLE_MAP = {
    'OSHAWA': 'dbo.DriveTecOshawaSales',
    'SUDBURY': 'dbo.DriveTecSudburySales'
}

def get_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f'https://login.microsoftonline.com/{TENANT_ID}',
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(
        scopes=[f'{GRAPH_ENDPOINT}/.default']
    )
    if 'access_token' not in result:
        raise Exception(f'Auth failed: {result.get('error_description')}')
    print('Authentication successful')
    print(f"Token expires in: {result['expires_in']} seconds")
    return result['access_token']

def get_unread_emails(token, processed_ids=set()):
    headers = {
        "Authorization": f"Bearer {token}",
        "ConsistencyLevel": "eventual"
    }
    url = (
        f"{GRAPH_ENDPOINT}/v1.0/users/{MAILBOX}/messages"
        f"?$filter=isRead eq false and hasAttachments eq true"
        f"&$select=id,subject,from,hasAttachments,receivedDateTime,body"
        f"&$top=20"
    )
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    all_emails = r.json().get("value", [])

    # Filter out any emails already processed in this session
    emails = [e for e in all_emails if e["id"] not in processed_ids]

    print(f"Found {len(all_emails)} unread email(s), {len(emails)} not yet processed.")
    return emails

def get_attachments(token, email_id):
    headers = {'Authorization': f'Bearer {token}'}
    url = f"{GRAPH_ENDPOINT}/v1.0/users/{MAILBOX}/messages/{email_id}/attachments"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    
    attachments = []
    for att in r.json().get('value', []):
        name = att.get('name', '')
        if not name.upper().endswith(('.CSV', '.XLSX')):
            continue
        
        file_bytes = base64.b64decode(att['contentBytes'])
        buffer = io.BytesIO(file_bytes)
        attachments.append({"name": name, "buffer": buffer})
        print(f'Loaded into memory: {name} ({len(file_bytes)} bytes)')
        
    return attachments

def clean_dataset_in_memory(attachments):
    REQUIRED_FILES = ['DISH-D.CSV', 'CUSTINFO.CSV', 'IREPORT (1).CSV']
    
    found = {req: None for req in REQUIRED_FILES}
    for att in attachments:
        name_upper = att['name'].upper()
        for req in REQUIRED_FILES:
            if req in name_upper:
                found[req] = att
                break
        
    missing = [req for req, val in found.items() if val is None]
    if missing:
        raise ValueError(
            f"Missing required file(s): {', '.join(missing)}. "
            f"Please attach DISH-D.CSV, CUSTINFO.CSV, and IREPORT.CSV in the same email."
        )

    print("  All 3 required files found. Running master report...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        
        for att in attachments:
            file_path = os.path.join(tmpdir, att['name'])
            with open(file_path, 'wb') as f:
                f.write(att['buffer'].getbuffer())
            print(f"  Saved {att['name']} to {file_path}")

        # Step 3: Run create_master_report.py as a subprocess from the temp dir
        script_path = os.path.join(BASE_DIR, "create_master_report.py")

        result = subprocess.run(
            [sys.executable, script_path],
            cwd=tmpdir,          # script runs inside temp dir, finds the CSVs there
            capture_output=True,
            text=True
        )

        if result.stdout:
            print("  [create_master_report.py stdout]:")
            print(result.stdout)
        if result.stderr:
            print("  [create_master_report.py stderr]:")
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"create_master_report.py failed with return code {result.returncode}"
            )

        output_path = os.path.join(tmpdir, "Master_Sales_Report.csv")

        if not os.path.exists(output_path):
            # List what IS in the temp dir to help debug
            print("  Files in temp directory:")
            for f in os.listdir(tmpdir):
                print(f"    - {f}")
            raise FileNotFoundError(
                "Master_Sales_Report.csv was not generated by the script."
            )

        output_buffer = io.BytesIO()
        with open(output_path, "rb") as f:
            output_buffer.write(f.read())
        output_buffer.seek(0)

        print("  Report loaded into memory. Temp files will now be deleted.")

    return "Master_Sales_Report.csv", output_buffer

# def reply_with_attachment(token, sender_email, original_subject, filename, buffer):
#     headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    
#     encoded = base64.b64encode(buffer.read()).decode('utf-8')
    
#     payload = {
#         "message": {
#             "subject": f"Re: {original_subject} — Cleaned Dataset",
#             "body": {
#                 "contentType": "Text",
#                 "content": (
#                     "Hi,\n\n"
#                     "Please find the cleaned dataset attached.\n\n"
#                     # "Cleaning steps applied:\n"
#                     # "  - Removed blank rows\n"
#                     # "  - Removed duplicate entries\n"
#                     # "  - Standardised column names\n\n"
#                     "Best regards,\n"
#                     "Automated Report Service"
#                 )
#             },
#             "toRecipients": [
#                 {"emailAddress": {"address": sender_email}}
#             ],
#             "attachments": [{
#                 "@odata.type": "#microsoft.graph.fileAttachment",
#                 "name": filename,
#                 "contentBytes": encoded
#             }]
#         }
#     }

#     url = f"{GRAPH_ENDPOINT}/v1.0/users/{MAILBOX}/sendMail"
#     r = requests.post(url, headers=headers, json=payload)
#     r.raise_for_status()
#     print(f"  Reply sent to: {sender_email}")

def get_db_connection() -> pyodbc.Connection:
    """
    Connect to the dashboard database using the app's service principal
    (CLIENT_ID + CLIENT_SECRET). No personal credentials needed.
    Requires ODBC Driver 18 for SQL Server to be installed.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={CLIENT_ID}@{TENANT_ID};"
        f"PWD={CLIENT_SECRET};"
        f"Authentication=ActiveDirectoryServicePrincipal;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    conn = pyodbc.connect(conn_str)
    print(f"  Connected to [{SQL_DATABASE}] on {SQL_SERVER}")
    return conn

def push_to_database(filename: str, buffer: io.BytesIO, email_body: str):
    email_body_upper = email_body.upper()
    target = None
    
    for keyword, table in LOCATION_TABLE_MAP.items():
        if keyword in email_body_upper:
            target = table
            break
        
    if target is None:
        raise ValueError(
            "Cannot determine target table from email body. "
            "Body must contain either 'Oshawa' or 'Sudbury'."
        )
    
    buffer.seek(0)
    df = pd.read_csv(buffer)
    df.columns = [c.strip().lstrip('\ufeff').replace(' ', '_') for c in df.columns]
    print(f"  Read {len(df)} rows from cleaned dataset.")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        
        columns = list(df.columns)
        placeholders = ', '.join(['?' for _ in columns])
        col_names = ', '.join([f'[{c}]' for c in columns])
        insert_sql = f"INSERT INTO {target} ({col_names}) VALUES ({placeholders})"
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        cursor.executemany(insert_sql, rows)
        conn.commit()
        print(f"  Inserted {len(rows)} rows into {target}.")
    except Exception as e:
        print(f"  Database error: {e}")
    finally:
        cursor.close()
        conn.close()
        print("  Database connection closed.")
    
    
def mark_as_read(token, email_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    url = f"{GRAPH_ENDPOINT}/v1.0/users/{MAILBOX}/messages/{email_id}"

    response = requests.patch(url, headers=headers, json={"isRead": True})

    print("PATCH:", response.status_code, response.text)

    # verify
    check = requests.get(url, headers=headers)
    print("isRead now:", check.json().get("isRead"))
        
def run():
    token = get_token()
    processed_ids = set()
    emails = get_unread_emails(token, processed_ids)

    if not emails:
        print("No new emails to process.")
        return

    for email in emails:
        email_id     = email['id']
        subject      = email.get('subject', '(No Subject)')
        sender_email = email['from']['emailAddress']['address']
        email_body   = email.get('body', {}).get('content', '')

        print(f"\nProcessing email from: {sender_email} | Subject: {subject}")
        processed_ids.add(email_id)

        attachments = get_attachments(token, email_id)

        if not attachments:
            print("  No valid attachments found. Marking as read and skipping.")
            mark_as_read(token, email_id)   # not a relevant email, ignore permanently

        try:
            cleaned_name, output_buffer = clean_dataset_in_memory(attachments)
            push_to_database(cleaned_name, output_buffer, email_body)
            mark_as_read(token, email_id)   # ← only mark as read if fully successful

        except ValueError as e:
            # Wrong files attached — not worth retrying, mark as read permanently
            print(f"  Skipping email (wrong files): {e}")
            # mark_as_read(token, email_id)

        except Exception as e:
            # Processing or DB error — mark as unread so it retries next run
            import traceback
            print(f"  Error processing email: {e}")
            print(traceback.format_exc())
            print("  Left as unread for retry on next run.")
            # do NOT call mark_as_read here

    print("\n=== Pipeline complete ===")
    
if __name__ == "__main__":
    run()
                