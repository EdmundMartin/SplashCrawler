import requests
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

class SplashScraper:

    def __init__(self, base_url):

        self.base_url = base_url
        self.root_url = '{}://{}'.format(urlparse(self.base_url).scheme, urlparse(self.base_url).netloc)
        self.user_agent = user_agent
        self.pool = ThreadPoolExecutor(max_workers=20)
        self.scraped_pages = set([])
        self.to_crawl = Queue()
        self.splash_server = ''
        self.to_crawl.put(self.base_url)

    def parse_links(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a', href=True)
        found_urls = []
        for link in links:
            url = link['href']
            if url.startswith('/') or url.startswith(self.root_url):
                url = urljoin(self.root_url, url)
                if url not in self.scraped_pages:
                    self.to_crawl.put(url)

    def scrape_info(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        products = soup.find_all('div', {'class': 'product-detail'})
        if products:
            for product in products:
                print(product)
                name = product.find('p', {'class':'margin-bottom-xxl'})
                price = product.find('div', {'class': 'price'})
                if name and price:
                    with open('product-details.csv', 'a') as output:
                        output.write('"{}","{}"\n'.format(name.get_text(), price.get_text()))

    def post_scrape_callback(self, res):
        result = res.result()
        if result.status_code == 200:
            self.parse_links(result.text)
            self.scrape_info(result.text)

    def scrape_page(self, url):
        res = requests.get('http://localhost:8050/render.html?url={}&timeout=30&wait=10'.format(url))
        return res

    def run_scraper(self):
        while True:
            try:
                target_url = self.to_crawl.get(timeout=120)
                if target_url not in self.scraped_pages:
                    print("Scraping URL: {}".format(target_url))
                    self.scraped_pages.add(target_url)
                    job = self.pool.submit(self.scrape_page, target_url)
                    job.add_done_callback(self.post_scrape_callback)
            except Empty:
                return
            except Exception as e:
                print(e)
                continue

if __name__ == '__main__':
    s = SplashScraper("http://www.boden.co.uk")
    s.run_scraper()
