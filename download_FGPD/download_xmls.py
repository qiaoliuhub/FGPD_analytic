import os
import re
import logging
import requests
import urllib
from time import sleep
import zipfile
import io
import ConfigParser 

from bs4 import BeautifulSoup

from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger('cir.custom')


class Command(BaseCommand):


    def extract_real_text(self, input_text):
        # Strips out FPDS img tag
        real_text = re.search(r'([A-Z0-9\.\-]+)', input_text)
        if real_text:
            return real_text.group(0)
        else:
            return None

    def get_links(self, html_string):
        # Parse html file and get the useful links
        soup = BeautifulSoup(html_string, 'html.parser')
        links = soup.findAll('a')
        return [(l.get('href'), self.extract_real_text(l.text)) for l in links]

    def make_directory(self, base_dir, new_dir_name):
        new_path = os.path.join(base_dir, new_dir_name)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        return new_path

    def handle(self, *args, **options):

        try:
            #Parse Config file
            Config = ConfigParser.RawConfigParser()
            Config.read("download_FGPD.cfg")

            working_dir = os.path.join(settings.BASE_DIR, 'data', 'fpds')

            BASE_SEARCH_URL = Config.get("SectionOne", "SearchURL")

            SCRAPE_HEADERS = {
                'User-Agent': Config.get("SectionOne", "UserAgent"),
                'From': Config.get("SectionOne", "From")
            }
            # Get the index page
            r = requests.get(BASE_SEARCH_URL, headers=SCRAPE_HEADERS)
            fy_links = self.get_links(r.text)
            sleep(1)

            for link in fy_links:
                if link[1]:
                    fy_path = self.make_directory(working_dir, link[1])

                    # Find links for this fiscal year
                    r = requests.get(os.path.join(BASE_SEARCH_URL, link[0]), headers=SCRAPE_HEADERS)
                    agency_links = self.get_links(r.text)
                    sleep(1)

                    for a in agency_links:
                        if a[1]:

                            # Find links for this agency
                            r = requests.get(os.path.join(BASE_SEARCH_URL, a[0]), headers=SCRAPE_HEADERS)
                            zip_links = self.get_links(r.text)
                            sleep(1)

                            # Download all the zipped XML files
                            for link in zip_links:
                                if link[1]:
                                    file_name = link[0].split('/')[-1].split('.')[0]
                                    download_path = os.path.join('https://www.fpds.gov/ddps/', link[0].replace('../', ''))
                                    destination_path = os.path.join(fy_path, file_name)
                                    if not os.path.exists(destination_path):
                                        logger.debug('Downloading %s...' % (download_path,))
                                        try:
                                            #Downloand and Extract the zipped file
                                            r = requests.get(download_path)
                                            z = zipfile.ZipFile(io.BytesIO(r.content))
                                            z.extractall(destination_path)
                                        except:
                                            if os.path.exists(destination_path):
                                                os.remove(destination_path)
                                                logger.debug('Deleting last file in progress ...')
                                        sleep(1)
                                    else:
                                        logger.debug('File %s already exists. Skipping.' % (file_name,))

        except:
            raise

