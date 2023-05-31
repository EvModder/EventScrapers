import scrapy
import logging

import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Set the threshold for selenium to WARNING
from selenium.webdriver.remote.remote_connection import LOGGER as seleniumLogger
seleniumLogger.setLevel(logging.WARNING)
# Set the threshold for urllib3 to WARNING
from urllib3.connectionpool import log as urllibLogger
urllibLogger.setLevel(logging.WARNING)

from time import sleep

class EventsSpider(scrapy.Spider):
    name = "tentimes"
    all_event_urls = set()
    max_urls_per_category = 9999999

    #https://docs.scrapy.org/en/latest/topics/spiders.html#spiderargs
    def __init__(self, *args, **kwargs):
      super().__init__(*args, **kwargs)
      print('in tentimes.__init__()')
      logging.getLogger('scrapy').propagate = True
      self.log('in tentimes.__init__()')

    def start_requests(self):
        logging.getLogger('scrapy').propagate = True
        print('in tentimes.start_requests()')
        self.log('in tentimes.start_requests()')
        # From the homepage: https://10times.com/
        # "Browse By Category" > "View All"
        category_urls = [
            'https://10times.com/usa/education-training',
            'https://10times.com/usa/technology',
            'https://10times.com/usa/medical-pharma',
            'https://10times.com/usa/business-consultancy',
            'https://10times.com/usa/research',
            'https://10times.com/usa/finance',
            'https://10times.com/usa/engineering',
            'https://10times.com/usa/arts-crafts',
            'https://10times.com/usa/wellness-healthcare',
            'https://10times.com/usa/food-beverage',
            'https://10times.com/usa/building-construction',
            'https://10times.com/usa/entertainment',
            'https://10times.com/usa/agriculture-forestry',
            'https://10times.com/usa/power-energy',
            'https://10times.com/usa/waste-management',
            'https://10times.com/usa/fashion-accessories',
            'https://10times.com/usa/logistics-transportation',
            'https://10times.com/usa/automotive',
            'https://10times.com/usa/apparel-fashion',
            'https://10times.com/usa/electronics-electricals',
            'https://10times.com/usa/home-office',
            'https://10times.com/usa/security',
            'https://10times.com/usa/travel-tourism',
            'https://10times.com/usa/animals-pets',
            'https://10times.com/usa/baby-kids',
            'https://10times.com/usa/telecommunication',
            'https://10times.com/usa/packaging',
            'https://10times.com/usa/hospitality',
            'https://10times.com/usa/trade-promotion',
            'https://10times.com/usa',
            'https://10times.com/usa/tradeshows',
            'https://10times.com/usa/conferences',
            'https://10times.com/usa/workshops',

            'https://10times.com/usa?month=this%20week'
        ]
        maxurls = getattr(self, 'maxurls', '')
        if maxurls.isdigit(): self.max_urls_per_category = maxurls
        print('max urls to scrape per category: '+str(self.max_urls_per_category))

        urlindex = getattr(self, 'urlindex', '')
        if urlindex.isdigit(): category_urls = [ category_urls[int(urlindex)] ]
        for url in category_urls:
            print('requesting category page: '+url)
            yield scrapy.Request(url, callback=self.parse_category_page)

    # Given an Event Category page url, return individual event urls
    def parse_category_page(self, response):
      print('getting event urls for category url: ' + str(response.url))
      options = Options()
      options.add_argument("disable-infobars")
      options.add_argument("--disable-notifications")
      options.add_argument("--incognito")
      options.add_argument("--disable-extensions")
      options.add_argument("--disable-gpu")
      options.add_argument("--disable-infobars")
      options.add_argument("--disable-web-security")
      options.add_argument("--no-sandbox")
      options.add_argument("--headless")
      options.add_argument("--disable-dev-shm-using")
      options.add_argument("--disable-setuid-sandbox")
      self.driver = webdriver.Chrome(options=options)

      self.driver.get(response.url)
      last_height = self.driver.execute_script("return document.body.scrollHeight")
      num_events = 0
      while True:
        next_event_urls = response.xpath('//*[@data-ga-category="Event Listing"]')
        #print('----------\n\n thing:\n'+str(next_event_urls)+'\n\n')
        for item in next_event_urls:
            url = item.xpath('@href').extract()[0]
            if not url or url == '' or url in self.all_event_urls:
              continue
            #print('got event url: ' + str(url))
            self.all_event_urls.add(url) # ensure we don't crawl the same event twice
            yield scrapy.Request(url, callback=self.parse_event)
            num_events += 1
            if num_events == self.max_urls_per_category:
              break
        if num_events == self.max_urls_per_category:
          print("got max event urls for category: " + response.url + " ("+str(self.max_urls_per_category)+")\n... exiting.")
          break

        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(20) # scrolling is really slow! wait 20s for results to load
        new_height = self.driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
          print("got all event urls for category: " + response.url + " ("+str(num_events)+")\n... exiting.")
          break
        last_height = new_height
        print("scrolled to new height: " + str(new_height))
      # end while loop

    # Given HTML for event page, extract relevent data
    def parse_event(self, response):
      print('parsing event: '+response.url)
      venue_full_address,venue_state,venue_street_address = self.parse_address(response)
      start_date,end_date = self.parse_date(response)
      event_type = self.parse_event_type(response)
      title,subtitle = self.parse_title(response)
      venue_name, venue_city, venue_country, venue_lat_long = self.parse_venue(response)
      description = self.parse_description(response)
      
      try:
        categories = response.xpath('//*[h2="Category & Type"]/parent::*').xpath('.//i//following::a[1]/text()').getall() #array!
      except:
        categories = []
      try:
        organizer_name = ' '.join(response.xpath('//*[h2="Organizer"]/parent::*').xpath('.//i//following::a[1]/text()').getall())
      except:
        organizer_name = ''

      yield {
        'scraper_source_url': response.url,
        'title': title,
        'subtitle': subtitle,
        'venue_name': venue_name,
        'venue_city': venue_city,
        'venue_state': venue_state,
        'venue_country': venue_country,
        'venue_street_address': venue_street_address,
        'venue_full_address': venue_full_address,
        'venue_lat_long': venue_lat_long,
        'start_date': start_date,
        'end_date': end_date,
        'description': description,
        'categories': categories,
        'event_type': event_type,
        'event_url': '', #cant get this from tentimes 
        'organizer_name': organizer_name
      }



    def parse_address(self, response):
      try:
        # eg: 1340 W Washington Blvd, Chicago, IL 60607
        venue_full_address = response.css('#map_dirr').xpath('.//span/text()').get()
        vfa_split = venue_full_address.split(' ')
        venue_state = vfa_split[-2]
        # trims last 3 items (city, state, zip) and joins remainder as street addr
        venue_street_address = ' '.join(vfa_split[:-3]).rstrip(',')
      except:
        venue_full_address = ''
        venue_state =  ''
        venue_street_address = ''
      return [venue_full_address, venue_state, venue_street_address]
    
    def parse_date(self, response):
      # eg: <span content="2023-06-17" class="ms-1">17 Jun 2023</span>
      # eg: <span content="2023-09-01" data-localizer="ignore" class="ms-1">01 - 03 Sep 2023</span>
      try:
        start_date = response.css('.fa-clock').xpath('..').xpath('.//span/@content').getall()
      except:
        start_date = ''
      try:
        start_dash_end = response.css('.fa-clock').xpath('..').xpath('.//span/@text').getall()
        end_date = start_dash_end.split(' - ')[1]
        month_map = {
        'jan':'01', 'feb':'02', 'mar':'03', 'apr':'04', 'may':'05', 'jun':'06',
        'jul':'07', 'aug':'08', 'sep':'09', 'oct':'10', 'nov':'11', 'dec':'12'
        }
        # "03 Sep 2023" -> "2023-09-03"
        end_date = end_date[7:]+'-'+month_map[end_date[3:6]]+'-'+end_date[:2]
      except:
        end_date = ''
      return [start_date, end_date]
    
    def parse_event_type(self, response):
      # Can be things like 'Conference' or 'Trade Show'
      try:
        etype = response.xpath('//*[h2="Category & Type"]').get()
        event_type = etype.split('</i>')[1].split('<br>')[0].lstrip()  #lstrip() strips leading white. 
      except:
        event_type = ''
      return event_type

    def parse_title(self, response):
      try: title = response.xpath('//h1/text()').get()
      except: title = ''

      try: subtitle = response.css('.desc').xpath('.//strong/text()').get()
      except: subtitle = ''

      return [title, subtitle]
    
    def parse_venue(self, response):
      try:
        venue_name =  response.css('.fa-map-marker').xpath('..').xpath('.//text()').get()
        venue_city =  response.css('.fa-map-marker').xpath('..').xpath('.//a/text()').get()
      except:
        venue_name = ''
        venue_city = ''

      try:
        venue_country = response.css('.fa-map-marker').xpath('..').xpath('.//a/text()').getall()[1]
      except:
        venue_country = ''

      try:
        venue_lat_long = response.css('#map_dirr').xpath('.//a').css('.btn').xpath('.//@onclick').get()
      except:
        venue_lat_long = ''
      return venue_name, venue_city, venue_country, venue_lat_long

    def parse_description(self, response):
      try: description = ' '.join(response.css('.desc').xpath('.//text()').getall())
      except: description = ''

      if description == '':
        try: description = ' '.join(response.css('.about').xpath('.//text()').getall())
        except: description = ''

      if description == '':
        try: description = ' '.join(response.css('.main').xpath('.//text()').getall())
        except: description = ''
      return description
