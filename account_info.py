from pyspark.sql import SparkSession
import sys,datetime,json
from  pyspark.sql.functions import input_file_name

day_key=None
mo_key=None 
year_key=None
hour_key='99'
app_name=None
variableDict={}

def main():      
    global variableDict,app_name,day_key,hour_key,mo_key,year_key
    sparkSession = None
    strStartTime = None
    results = []
    try:
        dtStartTime = datetime.datetime.now()
        strStartTime = dtStartTime.strftime("%d-%m-%Y %H:%M:%S")    
        argumentDict={}
        # Print command line arguments
        for arg in sys.argv[1:]:
            split = arg.split("=",1)
            argumentDict[split[0]] = split[1]
            print arg
            
        # Check app_name
        app_name = getVariable(argumentDict, "app_name")
        # Check workflow_name
        workflow_name = getVariable(argumentDict, "workflow_name")
        # Check mo_day
        day_key = getVariable(argumentDict, "day_key")
        print('------------------------------------------------------------------------------------------------------------------------------------')
        print('day_key: ' + day_key)        
                       
        # Init spark session 
        sparkSession = SparkSession.builder.appName(app_name).config("spark.sql.crossJoin.enabled", "true").enableHiveSupport().getOrCreate();
        
        df = sparkSession.sql("""  
                                  SELECT  	 	
                                  	CAST(source_id AS STRING)					AS source_id,
                                  	CAST(service_nbr AS STRING)					AS service_nbr,
                                  	CAST(acct_srvc_instance_key AS STRING)		AS acct_srvc_instance_key,
                                  	CAST(prod_line_key AS STRING)				AS prod_line_key,
                                  	CAST(cust_name AS STRING)					AS cust_name,
                                  	CAST(corporate_ind AS STRING)				AS corporate_ind,
                                  	CAST(REPLACE(addr_line,'\"','') AS STRING)	AS addr_line,	 	
                                  	CAST(email AS STRING)						AS email,
                                  	CAST(contact_nbr AS STRING)					AS contact_nbr,
                                  	CAST(id_no AS STRING)						AS id_no,
                                  	CAST(id_issue_date AS STRING)				AS id_issue_date,
                                  	CAST(id_name AS STRING)						AS id_name,
                                  	CAST(birth_date AS STRING)					AS birth_date,
                                  	CAST(permanent_addr AS STRING)				AS permanent_addr,
                                  	CAST(id_issue_place AS STRING)				AS id_issue_place,
                                  	CAST(gender AS STRING)						AS gender,
                                  	CAST(age_range AS STRING)					AS age_range,
                                  	CAST(job AS STRING)							AS job,
                                  	CAST(id_family AS STRING)					AS id_family,
                                  	CAST(hometown AS STRING)					AS hometown,
                                  	CAST(ethnic AS STRING)						AS ethnic,
                                  	CAST(religion AS STRING)					AS religion,
                                  	CAST(nationality AS STRING)					AS nationality,
                                  	CAST(marital_status AS STRING)				AS marital_status,
                                  	CAST(temporary_residence_place AS STRING)	AS temporary_residence_place,
                                  	CAST(current_addr AS STRING)				AS current_addr,
                                  	CAST(insurance_code AS STRING)				AS insurance_code,
                                  	CAST(place_of_birth AS STRING)				AS place_of_birth,
                                  	CAST(household_owner_relation AS STRING)	AS household_owner_relation,
                                  	CAST(household_book_no AS STRING)			AS household_book_no,
                                  	CAST(household_owner AS STRING)				AS household_owner,
                                  	CAST(geo_state_key AS STRING)				AS geo_state_key,
                                  	CAST(geo_dstrct_key AS STRING)				AS geo_dstrct_key,
                                  	CAST(geo_city_key AS STRING)				AS geo_city_key,
                                  	CAST(vne_id AS STRING)						AS vne_id,
                                  	CAST(last_updt_dt AS STRING)				AS last_updt_dt,
                                  	CAST(last_updt_by AS STRING)				AS last_updt_by,
                                  	CAST(bsns_cluster_key AS STRING)			AS bsns_cluster_key,
                                  	CAST(bsns_rgn_key AS STRING)				AS bsns_rgn_key,
                                  	CAST(day_key AS STRING)						AS day_key,
                                    CAST(substr(day_key,1,6) AS STRING)						AS mo_key	 	  
                                  FROM MBF_BIGDATA.ADMD_ACCOUNT_INFO  WHERE DAY_KEY = """ + day_key)                                                                                 
        df.repartition(10).write.option("sep",",").option("header","true").option("emptyValue", "").mode("overwrite").csv("/DATAWH/TMP/SOLR/ACCOUNT_INFO")
        
    except Exception as err:
        raise
    finally:      
        #Spark stop  
        if(sparkSession != None):
            sparkSession.stop()           
     
    
def getVariable(argumentDict, name):  
    try:
        result = argumentDict[name]
        #print(result)
        return result
    except Exception as e:
        print(e)    
        #print("ERR------------------------------")
        #print(name + " argument is not exists")
        raise Exception(name + " argument is not exists") 
            
if __name__ == '__main__':
    main()