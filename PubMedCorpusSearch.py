import logging
from elasticsearch import Elasticsearch, helpers
import os
import requests
import pubmed_parser as pp
import xml.etree.ElementTree as ET

def connect_elasticsearch(HOST = 'localhost', scheme = 'http'):
    _es = None
    _es = Elasticsearch([{'host': HOST, 'port':9200, 'scheme': scheme}])
    if _es.ping():
        print('Conectado correctamente')
    else:
        print('No ha sido posible realizar la conexion')
    return _es

def create_index(es, IndexName):
    created = False
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "std_english":{
                        "type": "standard",
                        "stopwords": "_english_"
                    },
                    "mesh_analyzer": {
                        "type": "pattern",
                        "pattern": "[:;]"
                    }
                }
            }
        },
        "mappings":{
            "properties": {
                "pmid": {
                    "type": "text"
                },
                "title": {
                    "type":"text",
                    "analyzer": "std_english",
                },
                "abstract":{
                    "type":"text",
                    "analyzer": "std_english",
                },
                "mesh_terms":{
                    "type": "text",
                    "analyzer": "mesh_analyzer",
                },
            }
        }
    }
    try :        
        es.indices.create(index = IndexName, ignore=400,body = settings)
        print('Created Index')
        created = True
    except Exception as ex:
        print((str(ex)))
    finally:
        return created

def generate_action(articulos):
    for articulo in articulos:
        doc = {
            'id':articulo['pmid'],
            "pmid":articulo["pmid"],
            "title":articulo["title"],
            "abstract": articulo["abstract"],
            "mesh_terms": articulo["mesh_terms"]
        }
        yield doc

def search(es, IndexName, query):
    res = es.search(index = IndexName, body = query)  

if __name__ == '__main__':

    logging.basicConfig(level=logging.ERROR)
    es = connect_elasticsearch()

    IndexName = 'PubMedCorpus'
    create_index(es, IndexName)

    os.chdir("pruebita")
    archivos = os.listdir()
    print(archivos)
   
    # for archivo in archivos:
    #     lista = pp.parse_medline_xml(archivo,
    #                                 year_info_only = False,
    #                                 nlm_category = False, 
    #                                 author_list = False,
    #                                 reference_list = False)
    #     acciones = generate_action(lista) 
    #     try:
    #         helpers.bulk(es, index=IndexName, actions=acciones)
    #         print(f'{archivo} indexado correctamente')
    #     except Exception as ex:
    #         print(f'Error durante la indexación de {archivo}: {str(ex)}')
    
    os.chdir('..\ ')
    
    # Leemos los topics
    tree = ET.parse('topics2019.xml')
    root = tree.getroot()

    for topic in root:
    query = {
        "size": 1000,
        "query": {
            "bool": {
                "must": [
                    {"multi_match": {'query': topic.find('disease').text, "fields": ["title", "abstract", "mesh_terms"]}},
                    {"multi_match": {'query': topic.find('gene').text, 'fields': ['abstract', 'mesh_terms']}}
                ],
                "should": [
                    {'match': {'mesh_terms': topic.find('demographic').text.split(' ')[1]}}
                ]
            }
        }
    }
    resultados = search(es, IndexName=IndexName, query=query)
