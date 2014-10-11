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


name="location-load"
base_url = 'http://attendance.gov.in/reports/regemp/get_regloc_report/'
download_path ="./../downloads/organizations/"
scraper = scrapekit.Scraper('batch_'+name, config=config)


@scraper.task
def scrape_index():
    db = dataset.connect('sqlite:///./../database/data.sqlite')
    db_organizations = db["organizations"]
    organizations = []
    for org in db_organizations.find(download=0):
        org_id = org["org_id"]
        organizations.append(org_id)
    db.commit()

    for org_id in organizations:
        org_id_full = ('000000'+org_id)[-6:]
        print org_id_full

        org_location_csv_url = base_url+org_id_full
        resp = scraper.get(org_location_csv_url)
        if resp.status_code == 200:
            org_csv_file_name =download_path+org_id_full+".csv"
            text_file = open(org_csv_file_name, "w")
            text_file.write(resp.content)
            text_file.close()
        #for testing
        db_organizations.update({"download":1,"org_id":org_id }, ['org_id'])
        db.commit()
        print "done downloading="+str(org_location_csv_url)
        time.sleep(2)

scrape_index.run()
