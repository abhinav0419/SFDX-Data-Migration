import os
import json
import time
import subprocess
from subprocess import CalledProcessError

objectListFile = "objectList.txt"
orgFile = "orgList.json"
deleteFolder = "delete"

def retrieve(sobject):
    filePath = deleteFolder + "/" + sobject + ".csv"
    query = "SELECT Id FROM " + sobject
    execute("sfdx force:data:soql:query -u {0} -r csv -q \"{1}\" > {2}".format(destOrg, query, filePath))

def delete(fileName):
    sobject = fileName[:-4]
    execute("sfdx force:data:bulk:delete -s {0} -f {1}".format(sobject, fileName))

def execute(process):
    return subprocess.check_output(process, shell=True)

def read_file(file_path):
    with open(file_path, "r", encoding="UTF-8") as f:
        return json.load(f)

def writeFile(filePath, data):
    with open(filePath, "w+") as file:
        file.write(json.dumps(data, indent=4))

orgs = read_file(orgFile)
destOrg = orgs["destOrg"]

#To-Do | Convert to json file
listSObject = []
with open(objectListFile, "r") as objListing:
    for sobject in objListing.read().split("\n"):
        listSObject.append(sobject.strip())

if deleteFolder not in os.listdir():
    os.mkdir(deleteFolder)

for sobject in listSObject:
    print(sobject)
    #retrieve(sobject)

os.chdir(deleteFolder)
for fileName in os.listdir():
    delete(fileName)

if deleteFolder in os.listdir():
    os.rmdir(deleteFolder)