# sequence gathering data
 two apis are used:
    https://api.isuresults.eu/
    https://speedskatingresults.com/


 # Step 1
 https://api.isuresults.eu/

Get all Worldrecords and all skaters 

 GET ISU WorldRecords ==> WorldRecord_ISU_Staging for new records ==> Worldrecord_ISU for all records
 GET ISU Skaters  ==> Skaters_ISU_staging for all records ==> Skaters_ISU for all records

 # Step 2
 https://speedskatingresults.com

 For all ISU skaters get Speedskating ID, search on givenname and familyname. Use these Speedskating IDs to loop for the realted SeasonalBest
 
 https://speedskatingresults.com/api/json/skater_lookup.php accepts familyname and givenname

 


