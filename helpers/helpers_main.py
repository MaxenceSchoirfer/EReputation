import datawarehouse_helper as dwh
import datalake_helper as dl
import datamart_helper as dm

DWH = dwh.DataWarehouseHelper()
DL = dl.DatalakeHelper()
DM = dm.DataMartHelper("COCA")

#DWH.create_date_for_year(2022)
DM.insert_fact_record_twitter(2)