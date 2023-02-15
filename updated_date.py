import time,os,json
# from sql_query_fetching_second import obj,obj_sec
from datetime import date,datetime


class Current_things:
    def current_date(self):
        return (date.today())
    
    def current_time(self):
        return (datetime.now().strftime("%H:%M:%S"))

# def parquet_file_updation():
#     obj.data_frame_1()
#     obj_sec.data_frame_sec()
    
    