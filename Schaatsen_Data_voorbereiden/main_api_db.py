from helper_db_voorbereiden import make_db_connection, call_stored_procedure_insert
from get_ISU_DB import get_isu_worldrecord_db, get_isu_skater_db, get_isu_skater_personalbest_db
from get_SPSK_DB import get_all_isu_skaters_spsk, get_all_skaters_all_spsk_seasonalbest

engine=make_db_connection()

# STEP 1: 
# A. Get ISU worldrecords add to Staging table
get_isu_worldrecord_db(engine)
# B. Move from Staging to Production table
call_stored_procedure_insert(engine,"INSERT_01_ISU_worldrecord" )

# STEP 2: 
# A. Get ISU skaters add to Staging table
get_isu_skater_db(engine)
# B. Move from Staging to Production table
call_stored_procedure_insert(engine,"INSERT_02_ISU_skater" )

# STEP 3: 
# A. Get ISU personalbest to Staging table
get_isu_skater_personalbest_db(engine)
# B. Move from Staging to Production table
call_stored_procedure_insert(engine,"INSERT_03_ISU_personalbest" )

# STEP 4: Use ISU givenname / familyname  to get SPSK skaters
# A. Add SPSK skaters to Staging table
get_all_isu_skaters_spsk(engine)
# B. Move from Staging to Production table
call_stored_procedure_insert(engine,"INSERT_04_SPSK_skater" )

# STEP 5: 
# A. Use SPSK skaters to add seasonbest to Staging table
get_all_skaters_all_spsk_seasonalbest(engine)
# B. Move from Staging to Production table
call_stored_procedure_insert(engine,"INSERT_05_SPSK_seasonalbest" )

# STEP 6: Combine ISU and SPSK results
call_stored_procedure_insert(engine,"INSERT_06_SPSK_ISU_results_combined" )

