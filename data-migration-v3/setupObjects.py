# Setup objects: User
# Custom Settings: Update for individual orgs
# 11/4/2020 | Shreya

import os
import requests
import csv
import json

CS_FOLDER = ''
USER_FILE = ''
ERROR_FOLDER = 'errorlogs'
USER_ERROR_LOGS = 'errorlogs/User.json'
CS_ERROR_LOGS = 'errorlogs/CustomSetting.json'

AUTH_TOKEN = ''
TOKEN_TYPE = ''
INSTANCE_URL = ''
USERNAME = ''
PASSWORD = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
LOGIN_URL = 'https://test.salesforce.com/services/oauth2/token'

# ---------------- GENERIC METHODS --------------
#read json file
def readFile(filePath):
    with open(filePath, "r", encoding="UTF-8") as f:
        return json.load(f)
        
#write to json file
def writeFile(filePath, data):
    with open(filePath, "w+") as file:
        file.write(json.dumps(data, indent=4))

#convert csv to json
def parse_csv(folderName, fileName, ignoreFields):
    records = []
    sobject = fileName.split(".")[0]
    
    csvFileReader = open(folderName + '/' + fileName, "r", encoding="utf8")
    reader = csv.DictReader(csvFileReader)

    for row in reader:
        row["attributes"] = {
            "type" : sobject
        }

        for key, value in list(row.items()):
            if key in ignoreFields:
                del row[key]
        
        records.append(row)

    return records

#REST callout to Patch records
def update_records(records):
    if len(records) > 0:
        url = INSTANCE_URL + "/services/data/v45.0/composite/sobjects/"
        requestBody = {
            "allOrNone": False,
            "records" : records
        }

        errorFile = []
        results = requests.patch(url=url, headers=final_header(), data=json.dumps(requestBody)).json()
        for result in results:
            print(result)
            #if len(result["errorCode"]) > 0:
                #errorFile.append(result)
    
        return errorFile
# ---------------- GENERIC METHODS --------------

# --------- CONSOLE INPUTS ------------
def get_user_input():
    print("Enter Configurations File Name")
    fileName = input()
    getConfigurationDetails(fileName)

    if ERROR_FOLDER not in os.listdir():
        os.mkdir(ERROR_FOLDER)

    print("Enter Custom Setting Folder Name")
    global CS_FOLDER 
    CS_FOLDER = input()

    print("Enter User File Name")
    global USER_FILE 
    USER_FILE = input()    
# --------- CONSOLE INPUTS -----------

# ---------------- CONNECTION SETUP ----------------------

def getConfigurationDetails(fileName):
    CONFIGURATION_FOLDER = 'configurations'
    config_details = readFile(CONFIGURATION_FOLDER + "/" + fileName)

    global USERNAME
    USERNAME = config_details["username"]
    global PASSWORD
    PASSWORD = config_details["password"]
    global CONSUMER_KEY
    CONSUMER_KEY = config_details["consumer_key"]
    global CONSUMER_SECRET
    CONSUMER_SECRET = config_details["consumer_secret"]

def access_token():
    response = requests.post(url=LOGIN_URL, params=authenticate_header()).json()
    
    global INSTANCE_URL
    INSTANCE_URL = response['instance_url']
    global AUTH_TOKEN
    AUTH_TOKEN = response['access_token']
    global TOKEN_TYPE
    TOKEN_TYPE = response['token_type']
    
def authenticate_header():
    access_dict = {
        'grant_type': 'password',
        'client_id': CONSUMER_KEY,
        'client_secret': CONSUMER_SECRET,
        'username': USERNAME,
        'password': PASSWORD
    }

    return access_dict

def final_header():
    auth_header = {
        'content-type': 'application/json',
        'Authorization': TOKEN_TYPE + ' ' + AUTH_TOKEN
    }

    return auth_header
# ---------------- CONNECTION SETUP ----------------------

# ---------------- UPSERT USERS -------------------
def get_users():
    USER_FOLDER = 'users'
    records = []
    ignoreFields = [
        "NumberOfFailedLogins"
    ]

    response = requests.get(url='{0}/services/data/v45.0/sobjects/User/describe'.format(INSTANCE_URL), headers=final_header()).json()
    for field in response["fields"]:
        if (not field["createable"] or not field["updateable"] or field["type"]=="datetime" or field["type"]=="date") and field["name"]!="Id":
            ignoreFields.append(field["name"])
    
    return parse_csv(USER_FOLDER, USER_FILE, ignoreFields)

def update_users():
    results = update_records(get_users())

    if len(results) > 0:
        writeFile(USER_ERROR_LOGS, results)
# ---------------- UPSERT USERS -------------------

# ---------------- CUSTOM SETTINGS -----------------
def get_csv_records():
    records = []
    IGNORE_FIELDS = [
        "CreatedById",
        "CreatedDate",
        "IsDeleted",
        "LastModifiedById",
        "LastModifiedDate",
        "SystemModstamp"
    ]

    for csv_file in os.listdir(CS_FOLDER):
        records.append(parse_csv(CS_FOLDER, csv_file, IGNORE_FIELDS))

    return records

def update_custom_settings():
    results = update_records(get_csv_records())

    if len(results) > 0:
        writeFile(CS_ERROR_LOGS, results)                
# ---------------- CUSTOM SETTINGS -----------------


# ******************** MAIN EXECUTION *************** #
get_user_input()
access_token()
update_users()
update_custom_settings()

print("---- Process Completed ---")

# create error file --> error detials --> error record
# active user records only
# resetPassword() -- bulk --> option to run or not