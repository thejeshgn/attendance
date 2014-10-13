# coding:utf-8
import scrapekit
import dataset
from BeautifulSoup import BeautifulSoup
from urlparse import urljoin
import lxml 
from datetime import datetime
import time
import csv
from dateutil.parser import parse

config = {
  'threads': 1,
  'cache_policy': 'http',
  'data_path': 'data'
}


name="attendance-load"

download_path ="./../downloads/attendance_10OCT2014/"
attendance_date = datetime.strptime("2014-10-10","%Y-%m-%d")

scraper = scrapekit.Scraper('batch_'+name, config=config)

@scraper.task
def scrape_index():
    db = dataset.connect('sqlite:///./../database/data.sqlite')
    db_attendance = db["attendance"]
    db_org_location = db["org_location"]

    offices = []
    for org in db_org_location.find(loaded=0, download=1):
        org_id = org["org_id"]
        loc_id = org["loc_id"]
        office = {"org_id":org_id, "loc_id":loc_id}
        offices.append(office)
    db.commit()

    for office in offices:
        org_id = office["org_id"]
        loc_id = office["loc_id"]
        loc_csv_file_name =download_path+org_id+"-"+loc_id+".csv"
        print loc_csv_file_name
        all_insert_records = []
        with open(loc_csv_file_name, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            row_no = 0
            for row in csv_reader:
                if row_no == 0:
                    row_no = row_no + 1
                    continue
                row_no = row_no + 1
                in_time = None
                out_time = None
                in_time_hrs = None
                out_time_hrs =None
                working_hours = None
                #print row[4]
                #print row[5]
                print str(row_no)
                if row[4] != "" and row[4] != "0000-00-00 00:00:00":
                    in_time = datetime.strptime(row[4],"%Y-%m-%d %H:%M:%S")
                    in_time_hrs = (row[4])[11:16]
                    in_time_hrs = in_time_hrs.split(":")
                    in_time_hrs = float(in_time_hrs[0])+ float(in_time_hrs[1])/float(60)


                if row[5] != "" and row[5] != "0000-00-00 00:00:00":
                    out_time = datetime.strptime(row[5],"%Y-%m-%d %H:%M:%S")
                    out_time_hrs = (row[5])[11:16]
                    out_time_hrs = out_time_hrs.split(":")
                    out_time_hrs = float(out_time_hrs[0])+ float(out_time_hrs[1])/float(60)

                if  out_time is not None and in_time is not None:
                    working_hours = str(out_time-in_time)

                insert_record = {"emp_id": row[0], "emp_name" : unicode(row[1], 'utf-8'), "org_dept_id" : row[2], "designation" : unicode(row[3], 'utf-8'), "in_time" : in_time, "out_time" : out_time, "at_type" : row[6], "org_id" :org_id, "loc_id" :loc_id, "attendance_date":attendance_date, "in_time_hrs":in_time_hrs,"out_time_hrs":out_time_hrs, "working_hours":working_hours}
                all_insert_records.append(insert_record)
                #print str(insert_record)
                #db_attendance.insert(insert_record)



        db_attendance.insert_many(all_insert_records)
        db.commit()

        db_org_location.update({"loaded":1,"org_id":org_id, "loc_id" :loc_id }, ['org_id','loc_id'])
        db.commit()

scrape_index.run()
