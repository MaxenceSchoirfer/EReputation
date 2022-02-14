import data_warehouse_helper

database_helper = database_helper.DataWarehouseHelper()
database_helper.insert_fact_frequency(database_helper.id_twitter, 1, 1, 1, "like", 10, 0, 2, 12)
