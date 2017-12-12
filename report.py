from elasticsearch import Elasticsearch


es = Elasticsearch(['192.168.1.66'])


def url_left():
    url_list = []
    res = es.search(index="gargantua",
                    body={"query": {"term": {"visited": False}}},
                    from_=0,
                    size=0,
                    _source=["url", "visited"])

    numero = res['hits']['total']
    res = es.search(index="gargantua",
                    body={"query": {"term": {"visited": False}}},
                    from_=0,
                    size=numero,
                    _source=["url", "visited"])

    for hit in res['hits']['hits']:
        url = hit["_source"]["url"]
        if url.startswith('http'):
            url_list.append(url)

    url_list = list(set(url_list))

    return url_list


def url_crawled():
    url_list = []
    res = es.search(index="gargantua",
                    body={"query": {"term": {"visited": True}}},
                    from_=0,
                    size=0,
                    _source=["url", "visited"])

    numero = res['hits']['total']
    res = es.search(index="gargantua",
                    body={"query": {"term": {"visited": True}}},
                    from_=0,
                    size=numero,
                    _source=["url", "visited"])

    for hit in res['hits']['hits']:
        url = hit["_source"]["url"]
        if url.startswith('http'):
            url_list.append(url)

    url_list = list(set(url_list))

    return url_list


def bc_accounts():
    url_list = []
    res = es.search(index="gargantua",
                    body={"query": {"wildcard": {"bc_public": "*"}}},
                    from_=0,
                    size=0,
                    _source=["url", "visited", "bc_public"])

    numero = res['hits']['total']
    res = es.search(index="gargantua",
                    body={"query": {"wildcard": {"bc_public": "*"}}},
                    from_=0,
                    size=numero,
                    _source=["url", "visited", "bc_public"])

    for hit in res['hits']['hits']:
        url = hit["_source"]["url"]
        bc_acc = hit["_source"]["bc_public"]
        for item in bc_acc:
            url_list.append([item, url])

    return url_list


total_crawled = len(url_crawled())
left_crawled = len(url_left())
urls = bc_accounts()
print("Gargantua has explored {} web pages from a total of {} links.\nDetected {} bitcoin accounts.".format(total_crawled, left_crawled+total_crawled, len(urls)))
for item in urls:
    print(item)

