import scrapy 
import unicodedata
import re
import regex
import sqlite3
from scrapy.crawler import CrawlerProcess

class ao3spider(scrapy.Spider):
    name = "hpspider"
    
    allowed_domains = ['archiveofourown.org']
    
    
    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
        'AUTOTHROTTLE_ENABLED': True,
        'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        url = "https://archiveofourown.org/works?utf8=%E2%9C%93&work_search%5Bsort_column%5D=revised_at&work_search%5Bother_tag_names%5D=&work_search%5Bexcluded_tag_names%5D=&work_search%5Bcrossover%5D=F&work_search%5Bcomplete%5D=&work_search%5Bwords_from%5D=&work_search%5Bwords_to%5D=&work_search%5Bdate_from%5D=&work_search%5Bdate_to%5D=&work_search%5Bquery%5D=&work_search%5Blanguage_id%5D=en&commit=Sort+and+Filter&tag_id=Percy+Jackson+and+the+Olympians+-+Rick+Riordan"
            
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self,response):

        current_page = response.xpath('//ol[@class="pagination actions"]//li/span[@class="current"]/text()').extract() 
        self.logger.debug("current page: %s", current_page)
        next_page = response.xpath('//a[@rel="next"]/@href').get()     
        if next_page:
            self.logger.debug('next search page: %s', next_page)
            if '5000' in current_page:             # hier begrenze ich die gescrapten fanfictions auf eine bestimmte Anzahl
                     return
            yield response.follow(next_page, self.parse)
    
    
        story_urls = response.xpath('//li/div/h4/a[1]/@href').getall()
        for story_url in story_urls:
            self.logger.debug('fanfic link: %s', story_url)
            yield response.follow(url=story_url + '?view_full_work=true', callback=self.parse_fanfic)

            
       
    def parse_fanfic(self, response):
        
        confirmation = response.xpath('//a[text()="Proceed"]')
        if confirmation:                                    # Warnung ignorieren
            self.logger.debug('Adult content, confirmation needed!')
            href = confirmation.attrib['href']
            yield response.follow(href, self.parse_fanfic)
            return
        
        ao3id = re.search(r'(?:/works/)([0-9]+)', response.url).group(1)
        
        title = response.xpath('//h2[@class="title heading"]/text()').get()
        if title is not None:
            title = title.strip()                       
        
        data = response.xpath('//*[@id="chapters"]//p/text()').getall()
        data = " ".join(data)
        data = unicodedata.normalize("NFKD", data)
        data = data.replace(r'\n',' ')
        data = data.replace(r'\t','')
        data = data.replace(r'\s','')
        data = data.replace(r'(See the end of the chapter for   .)','')
        data = data.strip()
        content = data

        author = response.xpath('//h3/a[@rel="author"]/text()').getall()
        author = ", ".join(author)

        metagroup = response.xpath('//dl[@class="work meta group"]')
                      
        tags = metagroup.css('dd.freeform.tags li ::text').getall()
        tags = ", ".join(tags)

        rating = response.xpath('//dd[@class="rating tags"]/ul/li/a/text()').get()

        category = response.xpath('//dd[@class="category tags"]/ul/li/a/text()').get()

        relationships = response.xpath('//dd[@class="relationship tags"]/ul//li/a/text()').getall()
        relationships = ", ".join(relationships)

        warnings = response.xpath('/dd[@class="warning tags"]/ul//li/a/text()').getall()
        warnings = ", ".join(warnings)

        characters = response.xpath('//dd[@class="character tags"]/ul//li/a/text()').getall()
        characters = ", ".join(characters)

        words = response.xpath('//dd[@class="words"]/text()').get()

        chapters = response.xpath('//dd[@class="chapters"]/text()').get() 

        result = [ao3id, title, author, rating, warnings, relationships, characters, tags, content, words, chapters]
        cursor = db.cursor()
        cursor.execute("""
        INSERT INTO PJ 
        (AO3ID, Title, Author, Rating, Warnings, Relationships, Characters, Tags, Content, Words, Chapters) 
        VALUES(?, ?, ?, ?, ?, ?, ? , ? , ?, ?, ?)""", result)
        db.commit()


db = sqlite3.connect("fanfictions.db")
db.row_factory = sqlite3.Row
cursor = db.cursor()
cursor.execute("""
    CREATE TABLE PJ(
		ID INTEGER PRIMARY KEY,
        AO3ID INT,
        Title TEXT,
        Author TEXT,
        Rating TEXT,
        Warnings TEXT,
        Relationships TEXT,
        Characters TEXT,
        Tags TEXT,
        Content TEXT,
        Words INT,
        Chapters TEXT
    );
""")
db.commit()


process = CrawlerProcess()
process.crawl(ao3spider)
process.start()