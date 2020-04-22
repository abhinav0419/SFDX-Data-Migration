import os
import subprocess
import sys
import random
import requests
import json
import collections
import time
import csv
from subprocess import CalledProcessError 

schemaFolder = "schema/"
exportFolder = "export-"
csvFolder = "csv/"
orgFile = "orgList.json"
objectListFile = "objectList.txt"
mainPlan = "mainPlan.json"

start = time.time()

dictSchema = {}
dictSobjectRecords = {}
dictRefsToUpsert = {}
dictIdsParentId = {}
importIdsWithRef = {}
originalRefMapping = {}

listSObject = []
removeFields = [
    "CreatedDate",
    "LastModifiedDate"
]

#--------------------------- SCHEMA ---------------------------#
def getQuery(sobject):
    isSelfLookup = False

    cmd_list = "sfdx force:schema:sobject:describe -u {0} --json -s {1}".format(srcOrg, sobject)
    schema = subprocess.check_output(cmd_list, shell=True)
    dictSchema[sobject] = json.loads(schema.decode("UTF-8"))
    
    setJsonData(schemaFolder + sobject+".json", dictSchema[sobject])

    query = "SELECT Id"
    for field in dictSchema[sobject]["result"]["fields"]:
        if field["createable"] and field["name"] not in removeFields:
            if field["type"] == "reference" and field["referenceTo"][0] in listSObject:
                query += "," + field["relationshipName"] + ".Id"
            #To-Do | Owner - Name is same across orgs
            elif field["type"] != "reference":
                query += "," + field["name"]

    #inner query of any object in object file
    for child in dictSchema[sobject]["result"]["childRelationships"]:
        if child["childSObject"] in listSObject:
            if child["childSObject"] == sobject:
                getParentRefs(sobject, child["field"])
            else:
                fieldAPI = child["field"].replace("__c", "__r.Id") if child["field"].endswith("__c") else child["field"].replace("Id", ".Id") 
                query += ",(SELECT Id," + fieldAPI +" FROM "+ child["relationshipName"] +")"            
            
    query += " FROM "+ sobject
    #To-Do
    query += " LIMIT 10000" 
    
    print(sobject, "Query:", query)
    print("--------------------------")
    return query
    
def getParentRefs(sobject, lookupField):
    query = "SELECT Id,"+lookupField+" FROM "+sobject
    cmdList = "sfdx force:data:soql:query --json -u {0} -q \"{1}\"".format(srcOrg, query)
    sobjectQuery = subprocess.check_output(cmdList, shell=True)
    
    records = json.loads(sobjectQuery.decode("UTF-8"))
    dictIdsParentId[sobject] = []
    
    for record in records["result"]["records"]:
        dictIdsParentId[sobject].append(record["Id"])

    setJsonData("RefIds.json", dictIdsParentId)

#--------------------------- EXPORT ---------------------------#
def export(sobject):
    query = getQuery(sobject)
    cmdList = "sfdx force:data:tree:export -u {0} -d {1} -p -q \"{2}\"".format(srcOrg, exportFolder + sobject, query)
    sobjectQuery = subprocess.check_output(cmdList, shell=True)

#combine multiple child files to 1 object file
def consolidateExports(sobject):
    parentFolder = exportFolder + sobject

    for childFile in os.listdir(parentFolder):
        childObject = childFile.split(".")[0][:-1]
        
        if childObject in listSObject and childObject != sobject:
            updateFile(parentFolder +"/"+ childFile, sobject, childObject)

#map refids to actual ids in src
def updateFile(srcPath, parentSObject, sobject):
    destPath = exportFolder + sobject + "/" + sobject +"s.json" 
    destData = dictSobjectRecords[sobject] if sobject in dictSobjectRecords else getJsonData(destPath)

    consolidateSobjectIds(srcPath, destData, parentSObject, sobject)
    dictSobjectRecords[sobject] = destData

#move refs from other folders to its folder
def consolidateSobjectIds(srcPath, destData, parentSObject, sobject):
    for field in dictSchema[sobject]["result"]["fields"]:
        if field["type"] == "reference" and field["referenceTo"][0] == parentSObject:
            lookupField = field["name"]
            lookupFieldRef = field["relationshipName"]    
            break

    #map lookups with @ref
    dictSrcIdRefs = {}
    for record in getJsonData(srcPath)["records"]:
        dictSrcIdRefs[record[lookupFieldRef]["Id"]] = record[lookupField]
    
    #delete hardcoded ids
    for record in destData["records"]:
        if lookupFieldRef in record and record[lookupFieldRef]["Id"] in dictSrcIdRefs:
            record[lookupField] = dictSrcIdRefs[record[lookupFieldRef]["Id"]]
            del record[lookupFieldRef]

            originalRefMapping[sobject].append(record)

#remove lookup if lookup object is in the below list
def updateOneOnOneReference(sobject):
    destPath = exportFolder + sobject + "/" + sobject +"s.json" 
    destData = dictSobjectRecords[sobject] if sobject in dictSobjectRecords else getJsonData(destPath)
    refFieldNames = []

    for child in dictSchema[sobject]["result"]["fields"]:
        if child["type"] == "reference" and containsParent(sobject, child["referenceTo"][0]):
            refFieldNames.append(child["name"])
        elif child["type"] == "reference" and child["referenceTo"][0] == sobject:
            resolveSelfReference(sobject, child["name"], destData)

    #remove parent refs from file
    if(len(refFieldNames) > 0) :
        dictRefsToUpsert[sobject] = []

        for record in destData["records"]:
            for fieldName in refFieldNames:
                if fieldName in record:                
                    ###dictRefsToUpsert[sobject].append(record)
                    originalRefMapping[sobject].append(record)
                    
                    del record[fieldName]

    dictSobjectRecords[sobject] = destData
    
def resolveSelfReference(sobject, lookupField, destData):
    lookupFieldRef = lookupField.replace("__c", "__r")

    for record in destData["records"]:
        if lookupFieldRef in record:
            parentId = record[lookupFieldRef]["Id"]
            index = dictIdsParentId[sobject].index(parentId)

            record[lookupField] = "@" + sobject + "Ref" + str(index+1)
            del record[lookupFieldRef]
            originalRefMapping[sobject].append(record)
            
    setJsonData("Output.json", originalRefMapping[sobject])
    
    for record in destData["records"]:
        if lookupField in record:
            del record[lookupField]
    
    originalRefMapping[sobject] = getJsonData("Output.json")

def containsParent(sobject, parentSobject):
    index = listSObject.index(sobject)
    tempList = listSObject[index+1 : len(listSObject)]

    return parentSobject in tempList


#-------------------------- PLAN ---------------------------#
def createPlan():
    data = []

    for sobjName in listSObject:
        objRef = {}
        objRef["sobject"] = sobjName
        objRef["saveRefs"] = True
        objRef["resolveRefs"] = True
        objRef["files"] = getFiles(sobjName)

        data.append(objRef)

    setJsonData(mainPlan, data)

def getFiles(sobject):
    files = []
    filePath = exportFolder + sobject + "/" + sobject +"s.json"
    data = dictSobjectRecords[sobject] if sobject in dictSobjectRecords else getJsonData(filePath)

    print(sobject, "has", len(data["records"]), "records")
    if len(data["records"]) > 200:
        #write to multiple files
        chunkData = list(chunkify(data["records"], 200))
        
        for i in range(len(chunkData)):
            fileName = exportFolder + sobject + "/" + sobject +"s" + str(i) + ".json"
            tempDict = {}
            
            tempDict["records"] = chunkData[i]
            setJsonData(fileName, tempDict)
            files.append(fileName)
    elif sobject in dictSobjectRecords:
        setJsonData(filePath, data)
        files.append(filePath)
    else:
        files.append(filePath)

    return files

#divide into 200 blocks
def chunkify(records, chunk_size):
    for i in range(0, len(records), chunk_size):
        yield records[i:i+chunk_size]

#----------------------- IMPORT ------------------#
def importData(dest_org):
    cmdList = "sfdx force:data:tree:import --json -u {0} -p {1}".format(dest_org, mainPlan)
    recordIds = subprocess.check_output(cmdList, shell=True)
    importIdsWithRef = json.loads(recordIds.decode("UTF-8"))
    
    #To-Do
    setJsonData("ImportedIds.json", importIdsWithRef)

#create csv per object and upsert them
def resolveLookups():
    importIdsWithRef = getJsonData("ImportedIds.json")
    mapSobjectImportIds = {}
    for sobject in listSObject:
        mapSobjectImportIds[sobject] = []
    dictRefIdWithRecordId = {}

    for record in importIdsWithRef["result"]:
        dictRefIdWithRecordId["@"+ record["refId"]] = record["id"]
        mapSobjectImportIds[record["type"]].append(record)
    
    #create csv sobject by sobject
    for sobject in listSObject:
        print("Calling replace refs")
        replaceRefs(sobject, dictRefIdWithRecordId)
        
        if len(mapSobjectImportIds[sobject]) > 0 and sobject in originalRefMapping and len(originalRefMapping[sobject]) > 0:
            toCSV(sobject, mapSobjectImportIds[sobject])

def replaceRefs(sobject, dictRefIdWithRecordId):
    print("In replace refs")
    refFieldNames = []

    for child in dictSchema[sobject]["result"]["fields"]:
        if child["type"] == "reference" and child["referenceTo"][0] in listSObject:
            refFieldNames.append(child["name"])

    for record in originalRefMapping[sobject]:
        for fieldName in refFieldNames:
            if fieldName in record and record[fieldName] in dictRefIdWithRecordId:
                record[fieldName] = dictRefIdWithRecordId[record[fieldName]]

def toCSV(sobject, importIds):
    csvFile = open(csvFolder + sobject + ".csv", "w+", newline="")
    csvWriter = csv.writer(csvFile)
    rows = []

    #for header
    header = ["Id"]
    for recordRef in originalRefMapping[sobject]:
        for attri in recordRef.keys():
            if attri != "attributes" and attri not in header:
                header.append(attri)

    print("header", header)
    csvWriter.writerow(header)

    #for row
    for record in importIds:
        for recordRef in originalRefMapping[sobject]:
            if recordRef["attributes"]["referenceId"] == record["refId"]:
                row = [record["id"]]

                for attri in header:  
                    if attri != "Id":                     
                        if attri in recordRef:
                            row.append(recordRef[attri])
                        else:
                            row.append("")
                csvWriter.writerow(row)
    
    csvFile.close()

def upsertRecords(destOrg):
    for csvFile in os.listdir(csvFolder):
        sobject = csvFile.split(".")[0]
        upsertJobIds = {}

        print("Upserting for", sobject)
        
        cmd_list = "sfdx force:data:bulk:upsert -u {0} --json -s {1} -f {2} -i Id".format(destOrg, sobject, csvFolder+csvFile)
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
   
#dest orgs
def destination_orgs():
    return getJsonData(orgFile)["destOrg"]

#--------------------------MAIN EXECUTION---------------------------#

#------ EXPORT ------#
execution_time("Export Started")
srcOrg = getJsonData(orgFile)["srcOrg"]

with open(objectListFile, "r") as objListing:
    List = objListing.read().split("\n")
    
    for sobject in List:
        listSObject.append(sobject.strip())

for sobject in reversed(listSObject):
    originalRefMapping[sobject] = []
    export(sobject)

for sobject in listSObject:
    consolidateExports(sobject)

for sobject in listSObject:
    updateOneOnOneReference(sobject)

setJsonData("Output.json", originalRefMapping)
execution_time("Export Ended")

#------ PLAN ------#
execution_time("Creating plan")
createPlan()
execution_time("Plan Created")

#------ IMPORT ------#
for destOrg in getJsonData(orgFile)["destOrg"]:
    execution_time("Importing has started for "+ destOrg)
    importData(destOrg)
    resolveLookups()
    upsertRecords(destOrg)
    execution_time("Importing has ended for "+ destOrg)

execution_time("*PROCESS FINISHED*")