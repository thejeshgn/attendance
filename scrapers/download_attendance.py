import scrapekit
import dataset
from BeautifulSoup import BeautifulSoup
from urlparse import urljoin
import lxml 
from datetime import datetime
import time

config = {
  'threads': 1,
  'cache_policy': 'http',
  'data_path': 'data'
}


name="attendance-download"
base_url = 'http://attendance.gov.in/reports/regemp/get_regemployee_report/'
download_path ="./../downloads/attendance_10OCT2014/"
scraper = scrapekit.Scraper('batch_'+name, config=config)


@scraper.task
def scrape_index():
    db = dataset.connect('sqlite:///./../database/data.sqlite')
    db_org_location = db["org_location"]
    print "**************** starting the scraper ***********************"
    locations = []
    for org in db_org_location.find(download=0):
        org_id = org["org_id"]
        loc_id = org["loc_id"]
        print str(org_id)        
        locations.append({"org_id":org_id,"loc_id":loc_id})
    db.commit()
    count = 0
    for org in locations:
        count = count+1
        org_id = org["org_id"]
        loc_id = org["loc_id"]
        org_id_full = ('000000'+org_id)[-6:]
        loc_id_full = ('000000'+loc_id)[-6:]
        #print org_id_full
        #print loc_id_full
        attendance_url = base_url+org_id_full+"/"+loc_id_full
        print "Try downloading="+str(attendance_url)
        resp = scraper.get(attendance_url)
        if resp.status_code == 200:
            loc_csv_file_name =download_path+org_id_full+"-"+loc_id_full+".csv"
            text_file = open(loc_csv_file_name, "w")
            text_file.write(resp.content)
            text_file.close()
        print str(count)
        print {"download":1,"org_id":org_id, "loc_id":loc_id }
        db_org_location.update({"download":1,"org_id":org_id, "loc_id":loc_id }, ['org_id','loc_id'])
        db.commit()
        time.sleep(30)

scrape_index.run()
