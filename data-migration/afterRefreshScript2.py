# @author:      Shreya Bordia
# @project:     ADT
# @description: Migrate records for a set of objects from 1 sandbox to another
#               using SFDX and CSV
# @created:     27/12/2019
# @update :     9/3/2020 | Get child based on parent selected

import os
import subprocess
import json
import time
import csv
import sys
from subprocess import CalledProcessError


EXPORT_FOLDER = "export-"
CSV_FOLDER = "csv"
ORG_FILE = "orgList.json"
OBJECT_FILE = "objectList2.json"
MAIN_PLAN = "mainPlan.json"
# which fields to ignore for all objects
IGNORE_FIELDS = [
    "Id",
    "CreatedDate",
    "LastModifiedDate"
]
#chunk size for each file
ALLOWED_RECORDS_IN_FILE = 200
#total chars allowed in query **
ALLOWED_QUERY_LIMIT = 7650

# from OBJECT_FILE, limits etc
mapSobjectDetails = {}
# each object lookups
mapSobjectLookups = {}
# sobject => list of records
mapSobjectRecords = {}
# ids for each object
mapSobjectListIds = {}
#BEFORE IMPORT: Old Id => RefNo
mapIdRef = {}
#AFTER IMPORT: RefNo => New Id
mapRefIdImportId = {}
#map of records for CSV file
mapSobjectCSVRecords = {}


#PK chunking: created through CSV [where lookup obj is child]
listRefsRecords = []
#list of plan objects and their files
listPlan = []


# ------------------ UTILITY --------------------- #
def executeCommand(query):
    return json.loads(subprocess.check_output(query, shell=True).decode("UTF-8"))["result"]

#read json file
def readFile(filePath):
    with open(filePath, "r", encoding="UTF-8") as f:
        return json.load(f)

#write to json file
def writeFile(filePath, data):
    with open(filePath, "w+") as file:
        file.write(json.dumps(data, indent=4))

#time taken
def executionTime(process_name, startTime):
    endTime = time.time()
    print("--------------------"+ process_name + " Took: {0:0.1f} seconds -----------------------".format(endTime - startTime))
    return endTime

def getSobjectDetails():
    for record in readFile(OBJECT_FILE):
        mapSobjectDetails[record["name"]] = record
#--------------------------- SCHEMA ---------------------------#
def isQuerableField(field):
    #only creatable fields
    return ( 
        field["createable"] 
        and 
        field["name"] not in IGNORE_FIELDS 
        and 
        #get references only is parent exists in objectList
        (
            (
                field["type"] == "reference" 
                and 
                field["referenceTo"][0] in mapSobjectDetails
            ) 
            or 
            (
                field["type"] != "reference"
            )
        )
    )

# map of lookupFieldName to ref sobject
def isReferenceField(field):
    return (
        field["type"] == "reference" 
        and 
        field["referenceTo"][0] in mapSobjectDetails
    )

def isObjectIgnoredField(field, sobject):
    return (
        "ignoreFields" not in mapSobjectDetails[sobject]
        or
        len(mapSobjectDetails[sobject]["ignoreFields"]) == 0
        or
        field["name"] not in mapSobjectDetails[sobject]["ignoreFields"]
    )

def hasLimit(sobject):
    return (
        "limit" in mapSobjectDetails[sobject]
        and
        len(mapSobjectDetails[sobject]["limit"]) > 0
        and
        int(mapSobjectDetails[sobject]["limit"]) > 0
    )

#add in where clause and order bys
def getAdditionalConditions(sobject):
    orderBy = ""
    if "orderBy" in mapSobjectDetails[sobject] and len(mapSobjectDetails[sobject]["orderBy"]) > 0:
        for field, direction in list(mapSobjectDetails[sobject]["orderBy"].items()):
            orderBy += field + " " + direction + ","
        orderBy = " ORDER BY " + orderBy[:-1]

    limit = " LIMIT " + mapSobjectDetails[sobject]["limit"] if hasLimit(sobject) else ""

    return orderBy + limit

def getQuery(sobject):
    mapSobjectLookups[sobject] = {}
    mapSchema = executeCommand("sfdx force:schema:sobject:describe -u {0} --json -s {1}".format(srcOrg, sobject))

    query = "SELECT Id"
    for field in mapSchema["fields"]:
        if isQuerableField(field): 
            query += "," + field["name"] if isObjectIgnoredField(field, sobject) else "" 

        if isReferenceField(field):
            mapSobjectLookups[sobject][field["name"]] = field["referenceTo"][0]
    
    #inner query of any object in object file
    for child in mapSchema["childRelationships"]:
        if child["childSObject"] in mapSobjectDetails:
            query += ",(SELECT Id FROM "+ child["relationshipName"] +")"

    query += " FROM " + sobject + (" WHERE " + mapSobjectDetails[sobject]["whereClause"] if "whereClause" in mapSobjectDetails[sobject] and len(mapSobjectDetails[sobject]["whereClause"]) > 0 else "")
    return query

#--------------------------- EXPORT ---------------------------#
def executeQuery(query):
    return executeCommand("sfdx force:data:soql:query -u {0} --json -q \"{1}\"".format(srcOrg, query))["records"]

def export(sobject):
    if sobject not in mapSobjectRecords:
        mapSobjectRecords[sobject] = []

    query = getQuery(sobject)
    
    #where clausing
    if sobject in mapSobjectListIds:
        query = query + (" AND " if "WHERE" in query else " WHERE ") + "Id IN ('"
        charCount = len(query)
    
        print(charCount, query, "\n---")
        
        if charCount < ALLOWED_QUERY_LIMIT:
            chunkSize = int((ALLOWED_QUERY_LIMIT-charCount)/18)
            chunkData = list(chunkify(list(mapSobjectListIds[sobject]), chunkSize))
                
            for i in range(len(chunkData)):
                thisQuery = query + "','".join(chunkData[i]) + "')"
                mapSobjectRecords[sobject].extend(executeQuery(thisQuery))
            #clear the queried child records
            del mapSobjectListIds[sobject]
    else:
        query += getAdditionalConditions(sobject)
        print(query, "\n---")
        mapSobjectRecords[sobject] = executeQuery(query)

    getChildIds(sobject)
    
#To-Do | Do not iterate over objects that don't have a child
#map of childs ids for where clausing
def getChildIds(sobject):
    for record in mapSobjectRecords[sobject]:
        mapIdRef[record["Id"]] = None

        for attri, value in record.items():
            if not value is None:
                #parent lookups
                if attri in mapSobjectLookups[sobject]:
                    if value not in mapIdRef:
                        parentSobject = mapSobjectLookups[sobject][attri]
                        
                        if parentSobject not in mapSobjectListIds:
                            mapSobjectListIds[parentSobject] = set()

                        mapSobjectListIds[parentSobject].add(value[:-3])
                #child lookups
                if attri.endswith('__r'):
                    for childRecord in value["records"]:
                        child = value["records"][0]["attributes"]["type"]

                        #if does not exist 
                        if childRecord["Id"] not in mapIdRef:
                            if child not in mapSobjectListIds:
                                mapSobjectListIds[child] = set()

                            mapSobjectListIds[child].add(childRecord["Id"][:-3])

#convert id to refNo & delete null refs
def convertIdsToRefNo(sobject):
    count = 1

    for record in mapSobjectRecords[sobject]:
        refId = sobject+"Ref"+ str(count)
        record["attributes"]["referenceId"] = refId
        mapIdRef[record["Id"]] = refId

        del record["attributes"]["url"]
        del record["Id"]

        #remove null valued lookups & child r/s
        for key, value in list(record.items()):
            if key.endswith('__r') or value is None:
                del record[key]

        count += 1

#only parent has been queried, thus, map their @refNo
def mapParentLookups(sobject):
    if len(mapSobjectLookups[sobject]) > 0:
        for record in mapSobjectRecords[sobject]:
            hasChildAsParent = False
            obj = {}

            for field, fieldValue in list(record.items()):
                obj[field] = fieldValue

                #is lookup field
                if field in mapSobjectLookups[sobject]:
                    parentSobject = mapSobjectLookups[sobject][field]

                    if fieldValue in mapIdRef.keys() and not mapIdRef[fieldValue] is None:
                        record[field] = "@" + mapIdRef[fieldValue]
                    #when parent is below in mapSobjectDetails
                    elif parentSobject in mapSobjectDetails:
                        hasChildAsParent = True
                        del record[field]

            if hasChildAsParent:
                listRefsRecords.append(obj)

#when parent is child         
def resolveChildRecords():
    while len(mapSobjectListIds) > 0:
        for sobject, ids in list(mapSobjectListIds.items()):
            if len(mapSobjectListIds[sobject]) > 0:
                export(sobject)

#for sobject higher in the list, store locally in listRefsRecords                  
def resolveChildAsParent():
    if len(mapSobjectLookups) > 0 and len(listRefsRecords) > 0:
        for record in listRefsRecords:
            sobject = record["attributes"]["type"]

            for field in mapSobjectLookups[sobject]:
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
    parentDir = EXPORT_FOLDER + sobject
    filePath = parentDir + "/" + sobject +"s"
    if parentDir not in os.listdir():
        os.mkdir(parentDir)

    print(sobject, "has", len(mapSobjectRecords[sobject]), "records","\n------")
    if len(mapSobjectRecords[sobject]) > ALLOWED_RECORDS_IN_FILE:
        #write to multiple files
        chunkData = list(chunkify(mapSobjectRecords[sobject], ALLOWED_RECORDS_IN_FILE))
        
        for i in range(len(chunkData)):
            fileName = filePath + str(i) + ".json"
            tempDict = {}
            tempDict["records"] = chunkData[i]
            
            writeFile(fileName, tempDict)
            files.append(fileName)
    else:
        tempDict = {}
        tempDict["records"] = mapSobjectRecords[sobject]

        writeFile(filePath +"0.json", tempDict)
        files.append(filePath +"0.json")
    
    return files

#divide into 200 blocks
def chunkify(records, chunk_size):
    for i in range(0, len(records), chunk_size):
        yield records[i:i+chunk_size]

#----------------------- IMPORT ------------------#
def importData():
    importRecords = executeCommand("sfdx force:data:tree:import --json -u {0} -p {1}".format(destOrg, MAIN_PLAN))
    
    for record in importRecords:
        mapRefIdImportId[record["refId"]] = record["id"]

# ------------------- CSV ---------------------- #
#create csv per object and upsert them
def resolveLookups(sobject):
    for record in listRefsRecords:
        for attri, value in list(record.items()):
            value = str(value)
        
            #skip @ --> [1:]
            if value.startswith("@"):
                value = mapRefIdImportId[value[1:]]

def setCSVRecords():
    for record in listRefsRecords:
        sobject = record["attributes"]["type"]
        mapSobjectCSVRecords[sobject].append(record)

#for lookup below in the list, create csv and upsert
def toCSV(sobject):
    if CSV_FOLDER not in os.listdir():
        os.mkdir(CSV_FOLDER)

    if len(mapSobjectCSVRecords[sobject]) > 0:
        csvFile = open(CSV_FOLDER +"/" + sobject + ".csv", "w+", newline="")
        csvWriter = csv.writer(csvFile)
        rows = []

        #for header
        header = ["Id"]
        for recordRef in mapSobjectCSVRecords[sobject]:
            for attri in recordRef.keys():
                if attri != "attributes" and attri not in header:
                    header.append(attri)

        print("------header-----------", header)
        csvWriter.writerow(header)

        #for row
        for record in mapSobjectCSVRecords[sobject]:
            row = []
            row.append(mapRefIdImportId[record["attributes"]["referenceId"]])

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
def upsertRecords():
    upsertJobIds = {}

    for csvFile in os.listdir(CSV_FOLDER):
        sobject = csvFile.split(".")[0]
        print("CSV Import for", sobject)
        upsertJobIds[sobject] = executeCommand("sfdx force:data:bulk:upsert -u {0} --json -s {1} -f {2} -i Id".format(destOrg, sobject, CSV_FOLDER + "/" + csvFile))

    #to check status of job:
    writeFile("CSVResults.json", upsertJobIds)

#-------------------------- * MAIN EXECUTION * ---------------------------#
startTime = time.time()
orgs = readFile(ORG_FILE)
srcOrg = orgs["srcOrg"]
destOrg = orgs["destOrg"]
getSobjectDetails()

endTime = executionTime("Loading Time", startTime)

#--------- EXPORT ---------
for sobject in mapSobjectDetails:
    export(sobject)

#when ApnVdn1 -> Dnis1 -> ApnVdn2
resolveChildRecords()

for sobject in mapSobjectDetails:
    #iterate each record and handle lookups
    convertIdsToRefNo(sobject)
    mapParentLookups(sobject)
    listPlan.append(createPlan(sobject))

writeFile(MAIN_PLAN, listPlan)
resolveChildAsParent()

endTime = executionTime("Export Time", endTime)

#--------- IMPORT ---------
importData()
endTime = executionTime("Import Time", endTime)

#--------- CSV ---------
for sobject in mapSobjectDetails:    
    resolveLookups(sobject)
    mapSobjectCSVRecords[sobject] = []

setCSVRecords()
    
for sobject in mapSobjectDetails:    
    toCSV(sobject)

upsertRecords()
endTime = executionTime("CSV Time", endTime)

executionTime("*PROCESS FINISHED*", startTime)