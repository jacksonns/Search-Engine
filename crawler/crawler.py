from warcio.capture_http import capture_http
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
import requests
from bs4 import BeautifulSoup
from url_normalize import url_normalize
import urllib3
from reppy.robots import Robots
from concurrent.futures import ThreadPoolExecutor

import queue
import time
import sys
import threading

import constants


# AUXILIARY FUNCTIONS
# Return first 20 words from BeautifulSoup 'p' section 
def get_first_words(soup_obj):
    paragraph_contents = soup_obj('p')
    text = ''
    cont = 0
    for paragraph in paragraph_contents:
        if not ('https:' in str(paragraph.text)):
            for word in paragraph.text.split():
                text += '{} '.format(word)
                cont += 1
                if cont >= 20: return text
    return text

# Return title from BeautifulSoup object
def get_title(soup_obj):
    title = soup_obj.find('title')
    title.extract()
    return title.get_text()

# Print page info on debug mode
def json_output(url, soup, unix_time):
    output = ''
    output += '{{ \"URL\": \"{}\",\n'.format(url)
    output += '\"Title\": \"{}\",\n'.format(get_title(soup).strip('\n'))
    output += '\"Text\": \"{}\",\n'.format(get_first_words(soup))
    output += '\"Timestamp\": \"{}\" }}\n'.format(unix_time)
    print(output)

# Return depth of url based on number of '/'
def depth(url):
    depth = len(urllib3.util.parse_url(url).path.split("/")) - 1
    if url.endswith('/'): depth -= 1
    return depth

# Separate root from path of an url
def parse_url(url):
    parse = url.split('/')
    root = '/'.join(parse[:3])
    path = '/' + '/'.join(parse[3:])
    return root, path


class Crawler:
    '''
    Attributes:
    - self.frontier: collected urls to be crawled
    - self.visited: dict of visited urls; the key is
                    the root of url and the value is a list
                    of url paths
    - self.pages_limit: number of pages to be crawled
    - self.debug_mode: True if want to print pages info.
    - self.warc_write_lock: Lock the warc file writing
    '''
    def __init__(self, seeds_file, pages_limit, debug_mode):
        try:
            with open(seeds_file, 'r') as file:
                seeds = file.read().split('\n')
            self.frontier = queue.Queue()
            for seed in seeds: self.frontier.put(url_normalize(seed))
        except IOError:
            print("File [{}] not found".format(seeds_file))
            sys.exit(1)
        self.visited = dict()
        self.pages_limit = int(pages_limit)
        self.debug_mode = debug_mode
        self.warc_write_lock = threading.Lock()

    def robots_delay(self, url):
        try:
            robots = Robots.fetch(Robots.robots_url(url), timeout=3)
            if robots.allowed(url, 'my-user-agent'):
                delay = robots.agent('my-user-agent').delay
                if delay: return delay
                else: return 0.1 # 100 ms
        except:
            pass
        return 0

    def enqueue_outlinks(self, soup):
        for link in soup.find_all('a', href = True):
            if 'http' in link['href']:
                normalized_url = url_normalize(link['href'])
                aux_url = normalized_url
                if not aux_url.startswith('https'):
                    aux_url = aux_url.replace('http', 'https')
                self.frontier.put(normalized_url)

    def get_next_url(self):
        while True:
            url = self.frontier.get()
            root, path = parse_url(url)
            if root in self.visited:
                if ( len(self.visited[root]) < constants.BREADTH_LIMIT 
                    and path != '/'
                    and path not in self.visited[root] 
                    and depth(path) <= constants.DEPTH_LIMIT ):
                    break
            else: break
        return url

    def process_page(self, url):
        file_name = 'pages/pages.warc.gz'
        #self.warc_write_lock.acquire()
        try:
            with capture_http(file_name):
                page = requests.get(url, timeout=3)
            #self.warc_write_lock.release()
            unix_time = int(time.time())
            data = page.text
            if ('text/html' in page.headers.get('content-type')):
                soup = BeautifulSoup(data, 'html.parser')
                if self.debug_mode: json_output(url, soup, unix_time)
                self.enqueue_outlinks(soup)
        except requests.RequestException:
            #self.warc_write_lock.release()
            return

    def set_visited_url(self, url):
        root, path = parse_url(url)
        if not root.startswith('https'): root = root.replace('http', 'https')
        if root not in self.visited:
            if path == '/': self.visited[root] = []
            else: self.visited[root] = [path]
        else: self.visited[root].append(path)

    def run_crawler(self):
        #threads = ThreadPoolExecutor(max_workers=constants.MAX_THREAD_NUM)
        while self.pages_limit > 0:
            url = self.get_next_url()
            delay = self.robots_delay(url)
            if delay:
                self.set_visited_url(url)
                #threads.submit(self.process_page, url)
                self.process_page(url)
                self.pages_limit -= 1
