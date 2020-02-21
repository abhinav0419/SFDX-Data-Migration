# @author:      Shreya Bordia
# @project:     ADT
# @description: Migrate records for a set of objects from 1 sandbox to another
#               using SFDX and CSV
# @created:     27/12/2019

import os
import subprocess
import json
import time
import csv
import sys
from subprocess import CalledProcessError
#To-Do | from multiprocessing.dummy import Pool as ThreadPool

schemaFolder = "schema/"
exportFolder = "export-"
csvFolder = "csv"
orgFile = "orgList.json"
objectListFile = "objectList.txt"
mainPlan = "mainPlan.json"

dictRefsToUpsert = {}
dictIdsParentId = {}
originalRefMapping = {}

listSObject = []
listRefsRecords = []
mapIdRef = {}
mapSobjectLookupFields = {}
mapSobjectRecords = {}
mapRefIdImportId = {}
mapCSVRecords = {}

limit = sys.argv[1] if len(sys.argv) > 1 else 0

removeFields = [
    "Id",
    "CreatedDate",
    "LastModifiedDate",
    "Isbuildermodelhome__c",
    "TimeZoneSidkey__c",
    "SFFormattedTMZ__c"
]

#--------------------------- SCHEMA ---------------------------#
def getQuery(sobject):
    mapSobjectLookupFields[sobject] = {}

    cmd_list = "sfdx force:schema:sobject:describe -u {0} --json -s {1}".format(srcOrg, sobject)
    schema = subprocess.check_output(cmd_list, shell=True)
    mapSchema = json.loads(schema.decode("UTF-8"))["result"]

    query = "SELECT Id"
    for field in mapSchema["fields"]:
        #only creatable fields
        if field["createable"] and field["name"] not in removeFields and ((field["type"] == "reference" and field["referenceTo"][0] in listSObject) or (field["type"] != "reference")):
            query += "," + field["name"]

        # map of lookupFieldName to ref sobject
        if field["type"] == "reference" and field["referenceTo"][0] in listSObject:
            mapSobjectLookupFields[sobject][field["name"]] = field["referenceTo"][0]
            
    query += " FROM "+ sobject 
    print(sobject, "Query:", query)
    #for dev envr
    query += " LIMIT " + limit if int(limit) > 0 else ""

    return query

#--------------------------- EXPORT ---------------------------#
def export(sobject):
    query = getQuery(sobject)
    cmdList = "sfdx force:data:soql:query -u {0} --json -q \"{1}\"".format(srcOrg, query)
    sobjectQuery = subprocess.check_output(cmdList, shell=True)

    mapSobjectRecords[sobject] = json.loads(sobjectQuery.decode("UTF-8"))["result"]

#iterate each record and handle lookups
def consolidateLookups(sobject):
    recordRefs(sobject)
    mapParentLookups(sobject)

#delete null refs
#convert id to refNo
def recordRefs(sobject):
    count = 1

    for record in mapSobjectRecords[sobject]["records"]:
        refId = sobject+"Ref"+ str(count)
        record["attributes"]["referenceId"] = refId
        mapIdRef[record["Id"]] = refId

        del record["attributes"]["url"]
        del record["Id"]

        #remove null valued lookups
        for key, value in list(record.items()):
            if value is None:
                del record[key]

        count += 1

#To-Do
#only parent has been queried, thus, map their @refNo
def mapParentLookups(sobject):
    mapLookupFields = mapSobjectLookupFields[sobject]
    
    if len(mapLookupFields) > 0:
        for record in mapSobjectRecords[sobject]["records"]:
            hasChildAsParent = False
            obj = {}

            for key, value in list(record.items()):
                obj[key] = value

                if key in mapLookupFields:
                    parentSobject = mapLookupFields[key]

                    if value in mapIdRef.keys():
                        record[key] = "@" + mapIdRef[value]
                    #when parent is below in listSobject
                    elif parentSobject in listSObject:
                        hasChildAsParent = True
                        del record[key]
                    #when lookup has value but not in listSobject
                    else:
                        del record[key]

            if hasChildAsParent:
                listRefsRecords.append(obj)
            
#for sobject lower in the list, store locally in listRefsRecords                  
def resolveChildAsParent():
    if len(mapSobjectLookupFields) > 0 and len(listRefsRecords) > 0:
        for record in listRefsRecords:
            sobject = record["attributes"]["type"]

            for field in mapSobjectLookupFields[sobject]:
                if field in record:
                    record[field] = "@" + mapIdRef[record[field]]

#-------------------------- PLAN ---------------------------#
def createPlan(sobject):
    objRef = {}
    objRef["sobject"] = sobject
    objRef["saveRefs"] = True
    objRef["resolveRefs"] = True
    objRef["files"] = getFiles(sobject)

    return objRef

def getFiles(sobject):
    files = []
    parentDir = exportFolder + sobject
    filePath = parentDir + "/" + sobject +"s"
    if parentDir not in os.listdir():
        os.mkdir(parentDir)

    print(sobject, "has", len(mapSobjectRecords[sobject]["records"]), "records","\n------")
    if len(mapSobjectRecords[sobject]["records"]) > 200:
        #write to multiple files
        chunkData = list(chunkify(mapSobjectRecords[sobject]["records"], 200))
        
        for i in range(len(chunkData)):
            fileName = filePath + str(i) + ".json"
            tempDict = {}
            tempDict["records"] = chunkData[i]
            
            setJsonData(fileName, tempDict)
            files.append(fileName)
    else:
        setJsonData(filePath +".json", mapSobjectRecords[sobject])
        files.append(filePath +".json")
    
    return files

#divide into 200 blocks
def chunkify(records, chunk_size):
    for i in range(0, len(records), chunk_size):
        yield records[i:i+chunk_size]

#----------------------- IMPORT ------------------#
def importData(destOrg):
    cmdList = "sfdx force:data:tree:import --json -u {0} -p {1}".format(destOrg, mainPlan)
    recordIds = subprocess.check_output(cmdList, shell=True)
    importRecords = json.loads(recordIds.decode("UTF-8"))

    getImportedRecords(importRecords)

# ------------------- CSV ---------------------- #
def getImportedRecords(importRecords):
    for record in importRecords["result"]:
        mapRefIdImportId[record["refId"]] = record["id"]
    
    # Print ids and refids 
    setJsonData("ImportedIds.json", mapRefIdImportId)

#create csv per object and upsert them
def resolveLookups(sobject):
    if len(listRefsRecords) > 0:
        for record in listRefsRecords:
            for attri in record.keys():
                value = str(record[attri])
            
                #skip @ --> [1:]
                if value.startswith("@"):
                    record[attri] = mapRefIdImportId[value[1:]]

def setCSVRecords():
    for record in listRefsRecords:
        sobject = record["attributes"]["type"]
        mapCSVRecords[sobject].append(record)

#for lookup below in the list, create csv and upsert
def toCSV(sobject):
    if csvFolder not in os.listdir():
        os.mkdir(csvFolder)

    if len(mapCSVRecords[sobject]) > 0:
        csvFile = open(csvFolder +"/" + sobject + ".csv", "w+", newline="")
        csvWriter = csv.writer(csvFile)
        rows = []

        #for header
        header = ["Id"]
        for recordRef in mapCSVRecords[sobject]:
            for attri in recordRef.keys():
                if attri != "attributes" and attri not in header:
                    header.append(attri)

        print("------header-----------", header)
        csvWriter.writerow(header)

        #for row
        for record in mapCSVRecords[sobject]:
            row = []
            recordRef = record["attributes"]["referenceId"]

            if recordRef in mapRefIdImportId:
                row.append(mapRefIdImportId[recordRef])

                for attri in header[1:]:  
                    if attri in record:
                        value = str(record[attri])

                        if value.startswith("@"):
                            refId = value[1:]
                            if refId in mapRefIdImportId:
                                row.append(mapRefIdImportId[refId])
                        else:
                            row.append(value)                                
                    else:
                        row.append("")    

                csvWriter.writerow(row)
        csvFile.close()

#bulk insert each csv file
def upsertRecords(destOrg):
    for csvFile in os.listdir(csvFolder):
        sobject = csvFile.split(".")[0]
        upsertJobIds = {}

        print("Upserting for", sobject)
        
        cmd_list = "sfdx force:data:bulk:upsert -u {0} --json -s {1} -f {2} -i Id".format(destOrg, sobject, csvFolder + "/" + csvFile)
        upsertRecords = subprocess.check_output(cmd_list, shell=True)
        upsertJobIds[sobject] = json.loads(upsertRecords.decode("UTF-8"))

    setJsonData("UpsertResults.json", upsertJobIds)

    #to check status of job:
    
#---------------- GENERIC METHODS ------------------#
#read json file
def getJsonData(filePath):
    with open(filePath, "r", encoding="UTF-8") as f:
        return json.load(f)

#write to json file
def setJsonData(filePath, data):
    with open(filePath, "w+") as file:
        file.write(json.dumps(data, indent=4))

#time taken
def execution_time(process_name):
    print("--------------------{0}-----------------------".format(process_name))
    print("It Took:  {0:0.1f} seconds".format(time.time() - start))

def setOrgs():
    print("srcOrg", srcOrg, "listDestOrgs", listDestOrgs)

#--------------------------MAIN EXECUTION---------------------------#
start = time.time()
execution_time("PROCESS STARTED")

#------ EXPORT ------#
execution_time("Export Started")
orgs = getJsonData(orgFile)
srcOrg = orgs["srcOrg"]
listDestOrgs = orgs["destOrgs"]

with open(objectListFile, "r") as objListing:
    List = objListing.read().split("\n")
    
    for sobject in List:
        listSObject.append(sobject.strip())

# ------------------- MULTI-THREADING --------------------------- #
#import concurrent.futures
#with concurrent.futures.ThreadPoolExecutor(max_workers = len(listSObject)) as cd executor: 
#    for sobject in listSObject:
#        future = executor.submit(export, sobject) 

planData = []
for sobject in listSObject:
    export(sobject)
    consolidateLookups(sobject)
    planData.append(createPlan(sobject))

execution_time("Export finished")

setJsonData('mapIdRef.json', mapIdRef)
resolveChildAsParent()
setJsonData(mainPlan, planData)
setJsonData('listRefsRecords.json', listRefsRecords)

#import
execution_time("Import Started")
for org in listDestOrgs:
    importData(org)
    execution_time("Import for " + org + " has finished")

    execution_time("CSV Started")
    for sobject in listSObject:    
        resolveLookups(sobject)
        mapCSVRecords[sobject] = []
    
    setCSVRecords()
    
    for sobject in listSObject:    
        toCSV(sobject)

    upsertRecords(org)
    execution_time("CSV Finished")

execution_time("*PROCESS FINISHED*")