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


name="location-load"
download_path ="./../downloads/attendance_10OCT2014/"
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
                in_time = None
                out_time = None
                print row[4]
                print row[5]

                if row[4] != "" and row[4] != "0000-00-00 00:00:00":
                    in_time = datetime.strptime(row[4],"%Y-%m-%d %H:%M:%S")

                if row[5] != "" and row[5] != "0000-00-00 00:00:00":
                    out_time = datetime.strptime(row[5],"%Y-%m-%d %H:%M:%S")
                insert_record = {"emp_id": row[0], "emp_name" : row[1], "org_dept_id" : row[2], "designation" : row[3], "in_time" : in_time, "out_time" : out_time, "at_type" : row[6], "org_id" :org_id, "loc_id" :loc_id}
                all_insert_records.append(insert_record)
                print str(insert_record)
                #db_attendance.insert(insert_record)



        db_attendance.insert_many(all_insert_records)
        db.commit()

        db_org_location.update({"loaded":1,"org_id":org_id, "loc_id" :loc_id }, ['org_id','loc_id'])
        db.commit()

scrape_index.run()
