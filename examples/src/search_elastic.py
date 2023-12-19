import elasticsearch

def es_iterate_all_documents(es, index, pagesize=250, **kwargs):
    """
    Helper to iterate ALL values from
    Yields all the documents.
    """
    offset = 0
    while True:
        result = es.search(index=index, **kwargs, body={
            "size": pagesize,
            "from": offset
        })
        hits = result["hits"]["hits"]
        # Stop after no more docs
        if not hits:
            break
        # Yield each entry
        yield from (hit['_source'] for hit in hits)
        # Continue from there
        offset += pagesize

es = elasticsearch.Elasticsearch("http://127.0.0.1:9200")
for entry in es_iterate_all_documents(es, 'example'):
    print(entry) # Prints the document as stored in the DB

# result = es.search(index="callysto", body={"query":{"match_all":{}}})
# print(result)