# coding:utf-8
import scrapekit
import dataset
from BeautifulSoup import BeautifulSoup
from urlparse import urljoin
import lxml 
from datetime import datetime
import time
import csv

config = {
  'threads': 1,
  'cache_policy': 'http',
  'data_path': 'data'
}


name="location-load"
download_path ="./../downloads/organizations/"
scraper = scrapekit.Scraper('batch_'+name, config=config)
#http://attendance.gov.in/reports/regemp/emp_details/000225/000166
base_url ="http://attendance.gov.in/reports/regemp/emp_details/" 

@scraper.task
def scrape_index():
    db = dataset.connect('sqlite:///./../database/data.sqlite')
    db_organizations = db["organizations"]
    db_org_location = db["org_location"]

    organizations = []
    for org in db_organizations.find(loaded=0):
        org_id = org["org_id"]
        organizations.append(org_id)
    db.commit()

    for org_id in organizations:
        org_id_full = ('000000'+org_id)[-6:]
        print "Loading the organization ----------->"+org_id_full

        org_csv_file_name =download_path+org_id_full+".csv"
        all_insert_records = []
        with open(org_csv_file_name, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            row_no = 0
            for row in csv_reader:
                if row_no == 0:
                    row_no = row_no + 1
                    continue
                insert_record = {"loc_name":unicode(row[0], 'utf-8') , "loc_id":str(row[1]) , "t_reg":str(row[2]) , "org_id":str(row[3]) , "t_emp_active":str(row[4]) , "t_uid_verify":str(row[5]) , "t_uid_reject":str(row[6])}
                all_insert_records.append(insert_record)
                #db_org_location.insert(insert_record)



        db_org_location.insert_many(all_insert_records)
        db.commit()

        db_organizations.update({"loaded":1,"org_id":org_id }, ['org_id'])
        db.commit()

scrape_index.run()
