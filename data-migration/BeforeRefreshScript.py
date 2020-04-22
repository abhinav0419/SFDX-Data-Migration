BeforeRefreshScript:

-> Pull users (Retrieval query: Active & lastLogin < 30days
Include: Anand, Victoria, Irwin, ==> 
)
-> Custom Settings (Retrieve: Integration Settings)

Dev/DevPro/Partial


Affiliate__c --> P: - , C: Lead Convert, DNIS
APNVDN__c --> P: DNIS, C: DNIS
Call_Disposition__c --. P: Call Disposition, C: Call Disposition      
Call_Script__c --> I
DNIS__c           --> P: APNVDN;Affiliate, DNIS C: APNVDN, Alerts 
Geocoding__c --> I
Lead_Convert__c   --> P: Affiliate, C: DNIS
ManagerTown__c          --> P: Town
Package__c       --> C: Promo Package
Permit__c          --> I
Postal_Codes__c --> P: Town, C: TerritoryAssignment__c, Alerts
Town__c --> C: Postal_Codes__c, Alerts
ProfileQuestion__c --> P: ProfileQuestion__c, C: ProfileQuestion__c
Promo_Package__c --> P: Package, Promotion__c 
Promotion__c  --> P: , C: Promo_Package__c, DNIS
Real_Time_Payment__c --> I
SchedUserTerr__c
Territory__c      
TerritoryAlerts__c          
TerritoryAssignment__c
Trip_Fee__c
Equifax_Mapping__c
PartnerConfiguration__c

Affiliate__c 21 : R
APNVDN__c 6,086 : All (8k)
Call_Disposition__c 1,047 : R 
Call_Script__c 41 : R
DNIS__c 8,116: A
Geocoding__c 569,283 : R
Lead_Convert__c 284 : R
ManagerTown__c 943 : R
Package__c 12 : R
Permit__c 11,985 : A
Postal_Codes__c 130,217 : A
Town__c 510 : A
ProfileQuestion__c 85 : R
Promo_Package__c 12 : R
Promotion__c 2 : R
Real_Time_Payment__c 208,451 : R
SchedUserTerr__c 11,878 : A
Territory__c 10734   : A
TerritoryAlerts__c 1948  : R
TerritoryAssignment__c 165,086 : A
Trip_Fee__c 2 : R
Equifax_Mapping__c 38 : R
PartnerConfiguration__c 2 : R

Customer Web Questions: A 12
CPQ Order Type: 8 (Dev Pro) : R
UserAppointmentCounts: A 241,503

1,126, 783 records * 2 KB = 2.253566 GB

TYPE: Dev/DevPro/Partial: What all obj
ENV: Dev1/Dev2/Test1/Test2/Test3 --> Integration Settings (CS), Users (diff list for each)
*handle for full copy and partial too

Affiliate__c
APNVDN__c
DNIS__c
Lead_Convert__c
Permit__c
Town__c
Postal_Codes__c
SchedUserTerr__c
Territory__c
TerritoryAssignment__c
CustomerWebQuestion__c
User_Appointment_Count__c


#--------------------------MAIN EXECUTION---------------------------#
start = time.time()
execution_time("PROCESS STARTED")

#------ EXPORT ------#
execution_time("Export Started")
orgs = getJsonData(orgFile)
srcOrg = orgs["srcOrg"]
destOrg = orgs["destOrg"]

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
    execution_time("Exported " + sobject)    

execution_time("Export finished")

resolveChildAsParent()
setJsonData(mainPlan, planData)

#import
execution_time("Import Started")

importData()
execution_time("Import has finished")

execution_time("CSV Started")
for sobject in listSObject:    
    resolveLookups(sobject)
    mapCSVRecords[sobject] = []

setCSVRecords()

for sobject in listSObject:    
    toCSV(sobject)

upsertRecords()
execution_time("CSV Finished")

execution_time("*PROCESS FINISHED*")