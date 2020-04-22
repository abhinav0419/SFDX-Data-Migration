import os
import json
import time
import subprocess
from subprocess import CalledProcessError

objectListFile = "objects-partial.txt"
orgFile = "orgList.json"
deleteFolder = "delete"

# --------------- UTILITY -----------
def readfile(file_path):
    with open(file_path, "r", encoding="UTF-8") as f:
        return json.load(f)

def writefile(filePath, data):
    with open(filePath, "w+") as file:
        file.write(json.dumps(data, indent=4))

# ---------------- RETRIEVE -----------
def retrieve(sobject):
    query = "SELECT Id FROM " + sobject
    records = json.loads(subprocess.check_output("sfdx force:data:soql:query -u {0} --json -q \"{1}\"".format(destOrg, query), shell=True).decode("UTF-8"))

    if len(records["result"]["records"]) > 0:
        writefile(deleteFolder + "/" + sobject + ".json", records)

# -------------- DELETE --------------
def delete(fileName):
    sobject = fileName[:-5]
    print("deleting", sobject)
    subprocess.check_output("sfdx force:data:bulk:delete --json -s {0} -f {1}".format(sobject, fileName), shell=True)

# ------------- MAIN EXECUTION  -----------------
orgs = readfile(orgFile)
destOrg = orgs["destOrg"]

#To-Do | Convert to json file
listSObject = []
with open(objectListFile, "r") as objListing:
    for sobject in objListing.read().split("\n"):
        listSObject.append(sobject.strip())

if deleteFolder not in os.listdir():
    os.mkdir(deleteFolder)

for sobject in listSObject:
    print(sobject, "\n-----")
    retrieve(sobject)

os.chdir(deleteFolder)
for fileName in os.listdir():
    delete(fileName)