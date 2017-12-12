import requests
import bs4
import re
import hashlib
import logging
from blockchain import blockexplorer
from datetime import datetime
from elasticsearch import Elasticsearch

# URL of our elasticsearch cluster
es = Elasticsearch(['XXX.XXX.XXX.XXX'])
# Key for blockchain.info API
apic = 'XXXXX-XXXXX-XXXXX-XXXXX'


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y/%m/%d %H:%M:%S ',
                    filename='gargantua.log',
                    level=logging.ERROR)

def calculate_hash(html):
    return hashlib.sha224(html).hexdigest()


def extract_bitcoin_accounts(text):
    '''
    Extract posible bitcoin public keys
    :param text: Text to search the bitcoin public keys
    :return: List with bitcoin public keys, or None if there isn't
    '''
    # Regular expresion to detect bitcoin public keys. There aren't 0, I, O, and l
    regex = r"[1-3][123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]{27,34}"
    bclist = re.findall(regex, text)

    bclist = list(set(bclist))            # Delete duplicates
    possible = len(bclist)
    ret = []

    # Look for the word bitcoin also
    if "BITCOIN" in text.upper() and len(bclist) > 0:
        for address in bclist:
            try:
                salida = blockexplorer.get_address(address, limit=1, api_code=apic)
                if len(salida.transactions) > 0:
                    ret.append(address)
            except Exception as e:
                logging.error("In function extract_bitcoins_accounts/blockexplorer API: {}".format(e))
                pass
        if len(ret) > 0:
            return ret, possible
        else:
            return None, possible
    else:
        return None, possible


def is_new_url(url):
    '''
        Insert URL with data
        :param url: url to search
        :return: True if it doesn't exist in ES or false if it exists
    '''

    # Search the URL
    try:
        res = es.search(index="gargantua",
                        body={"query": {"term": {"url": url}}},
                        size=1)
    except Exception as e:
        logging.error("In function is_new_url: {}".format(e))
        return False

    total = res['hits']['total']

    if total > 0:
        return False
    else:
        return True


def delete_url(url):
    '''
        Insert URL with data
        :param url: url to crawl
        :return: Total urls in elastic, or none ir anything fails
    '''
    # Search the URL and store the id
    try:
        res = es.search(index="gargantua",
                        body={"query": {"term": {"url": url}}},
                        size=1)
    except Exception as e:
        logging.error("In function delete_urls: {}".format(e))
        return None

    total = res['hits']['total']

    if total > 0:
        id = res['hits']['hits'][0]['_id']
        # Delete the id
        try:
            es.delete(index="gargantua", doc_type='onionweb', id=id)
        except Exception as e:
            logging.error("In function delete_urls: {}".format(e))
            return None

    return total


def modify_url(url, visited=None, text=None, bc_public=None, date=None, html_hash=None):
    '''
        Insert URL with data
        :param url: url to crawl
        :param visited: True/False, if was visited before
        :param text: Text of the web
        :param bc_public: bitcoin public keys extracted
        :param date: date to modify
        :param html_hash: hash to modify
        :return: Total url located in elastic, or none ir anything fails
    '''

    # Search the URL and store the old data
    try:
        res = es.search(index="gargantua",
                        body={"query": {"term": {"url": url}}},
                        size=1)
    except Exception as e:
        logging.error("In function modify_urls, when search url: {}".format(e))
        return None

    total = res['hits']['total']

    if total > 0:
        doc = res['hits']['hits'][0]['_source']
        esid = res['hits']['hits'][0]['_id']

        # Check fields to modify in ES
        if visited is not None:
            doc["visited"] = visited
        if text:
            doc["text"] = text
        if bc_public:
            doc["bc_public"] = bc_public
        if date:
            doc["date"] = date
        if html_hash:
            doc["hash"] = html_hash

        # Delete the old one and create the new one
        try:
            #res = es.delete(index="gargantua", doc_type='onionweb', id=esid)
            res = es.index(index="gargantua", doc_type='onionweb', id=esid, body=doc)
        except Exception as e:
            logging.error("In function modify_urls: {}".format(e))
            return None


    return total


def insert_url(url, visited=False, text=None, bc_public=None, html_hash=None):
    '''
    Insert URL with data
    :param url: url to crawl
    :param visited: True/False, if was visited before
    :param text: Text of the web
    :param bc_public: bitcoin public keys extracted
    :return: True / False, if create one or any other problem
    '''
    doc = {
        'url': url,
        'visited': visited,
        'date': datetime.now(),
        'text': text,
        'bc_public': bc_public,
        'hash': html_hash
    }
    try:
        es.index(index="gargantua", doc_type='onionweb', body=doc)
        return True
    except Exception as e:
        logging.error("In function insert_urls: {}".format(e))
        return False


def initialize_es(delete=False):
    '''
    Initialize ES index, or delete it if delete flag is true
    :param delete: if true, delete gargantua indez
    :return: false if any exception was raised
    '''
    try:
        # delete index if exists and option is True
        if delete and es.indices.exists("gargantua"):
            es.indices.delete(index="gargantua")

        # If index doesn't exist, create it
        if not es.indices.exists("gargantua"):
            # index settings
            settings = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "onionweb": {
                        "properties": {
                            "url": {
                                "type": "keyword"
                            },
                            "visited": {
                                "type": "boolean"
                            }
                        }
                    }
                }
            }

            # create index
            es.indices.create(index="gargantua", ignore=400, body=settings)
            if insert_url('http://zqktlwi4fecvo6ri.onion/wiki/index.php/Main_Page'):
                logging.info("Initializing and creating index with only one entry")
    except Exception as e:
        logging.error("In function initialize_es: {}".format(e))


def url_heap():
    '''
    Returns a url to monitorize
    :return: url or None if there isn't anymore
    '''
    try:
        res = es.search(index="gargantua",
                        body={"sort": {"date": {"order": "asc"}},
                              "query": {"term": {"visited": False}}},
                        size=10,
                        _source=["url", "date"])
    except Exception as e:
        logging.error("In function url_heap: {}".format(e))
        return None

    total = res['hits']['total']
    if total > 0:
        url = res['hits']['hits'][0]["_source"]["url"]
        return url
    else:
        return None


def hashed_before(html_hash):
    '''
    Returns a url to monitorize
    :param html_hash: hash to search in ES
    :return: url or None if there isn't anymore
    '''
    try:
        res = es.search(index="gargantua",
                        body={"query": {"term": {"hash": html_hash}}},
                        size=1)
    except Exception as e:
        logging.error("In function hashed_before: {}".format(e))
        return None

    total = res['hits']['total']
    if total > 0:
        url = res['hits']['hits'][0]["_source"]["url"]
        return url
    else:
        return None


def extract_web(url):
    '''
    Extract html code from a web
    :param url: url to extract data
    :return: none or html content
    '''
    # User-Agent and proxies IP
    header = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1'}
    proxies = {
        'http': '192.168.1.66:8118',
        'https': '192.168.1.66:8118'
    }

    try:
        # Open conection and take the response and the html content
        res = requests.get(url, proxies=proxies, headers=header)
        html = res.text
    except Exception as e:
        logging.error("In function extract_web: {}".format(e))
        return None

    return html.encode('utf-8')


def extract_text(html):
    '''
    Extract plain text from a html page
    :param html: html page content
    :return: extracted text or None if anything fails
    '''
    try:
        soup = bs4.BeautifulSoup(html, 'html.parser')
        [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]

        # Clean spaces from the web
        text = soup.getText()
        visible_text = ' '.join(text.splitlines()).split(' ')
        visible_text = list(filter(lambda x: x!='', visible_text))
        visible_text = ' '.join(visible_text)
    except Exception as e:
        logging.error("In function extract_text: {}".format(e))
        visible_text = None

    return visible_text


def extract_urls(html, root):
    '''
    Extract urls from a html code
    :param html: html content
    :param root: root url to compose partial urls
    :return: list with all url founded
    '''
    links = []

    try:
        soup = bs4.BeautifulSoup(html, 'html.parser')
        tags = soup.find_all('a')

        for tag in tags:
            link = tag.get('href')
            if link is not None and len(link) > 0:
                if link[0] != '#':
                    if link[0] == '/':
                        links.append(root+link)
                    else:
                        links.append(link)
    except Exception as e:
        logging.error("In function extract_urls: {}".format(e))

    return links


def root_url(url):
    if url[-1] == '/':
        return url[:-1]
    if url.lower()[-4:] == 'html':
        return '/'.join(url.split('/')[:-1])
    else:
        return url


def worker(url):
    html = extract_web(url)
    if html:
        html_hash = calculate_hash(html)
        hb = hashed_before(html_hash)
        if hb:
            logging.info("El contenido de {} ha sido hasheado antes en {}".format(url, hb))
        text = extract_text(html)
        date = datetime.now()

        try:
            bhtml = html.decode('utf-8')
        except:
            modify_url(url, visited=True)
            logging.error("Can't decode {}".format(url))
            return

        bc_public, possible = extract_bitcoin_accounts(bhtml)
        if bc_public:
            logging.debug("{} possible bc address with {} checked addres in {}".format(possible, len(bc_public), url))
        if modify_url(url, text=text, date=date, bc_public=bc_public, visited=True, html_hash=html_hash):
            logging.info("{} processed".format(url))
        urls = extract_urls(html, root_url(url))
        if len(urls) > 0:
            counter = 0
            for item in urls:
                if is_new_url(item):
                    insert_url(item)
                    counter += 1
            logging.info("Located {} new urls in {}".format(counter, url))
    else:
        modify_url(url, visited=True)
        logging.error("{} failed to process when gargantua extract the html".format(url))


def main():
        # Initialize gargantua index
    initialize_es()

    # Search urls not visited in ElasticSearch
    while True:
        url = url_heap()
        print(url)
        if url:
            worker(url)


if __name__ == '__main__':
    main()
