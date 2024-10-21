from elasticsearch import Elasticsearch
import pandas as pd

# Initialize Elasticsearch
es = Elasticsearch("http://localhost:9200")

def create_collection(name):
    if not es.indices.exists(index=name):
        es.indices.create(index=name)
        print(f"Collection '{name}' created.")
    else:
        print(f"Collection '{name}' already exists.")

def index_data(collection, exclude_col):
    df = pd.read_csv("employee_data.csv")
    if exclude_col in df.columns:
        df = df.drop(columns=[exclude_col])
    for _, row in df.iterrows():
        es.index(index=collection, body=row.to_dict())
    print(f"Data indexed in '{collection}', excluding '{exclude_col}'.")

def search_by_column(collection, column, value):
    query = {
        "query": {
            "match": {column: value}
        }
    }
    results = es.search(index=collection, body=query)
    print(f"Results for '{column}': '{value}':")
    for hit in results['hits']['hits']:
        print(hit['_source'])

def get_emp_count(collection):
    count = es.count(index=collection)
    print(f"Employee count in '{collection}': {count['count']}")
    return count['count']

def del_emp_by_id(collection, emp_id):
    es.delete(index=collection, id=emp_id)
    print(f"Deleted employee ID: {emp_id}")

def get_dep_facet(collection):
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    results = es.search(index=collection, body=query)
    print(f"Department counts in '{collection}':")
    for bucket in results['aggregations']['department_count']['buckets']:
        print(f"{bucket['key']}: {bucket['doc_count']}")

# Define collections
name_collection = 'Hash_Kajapathy'
phone_collection = 'Hash_1234'

# Execute functions
create_collection(name_collection)
create_collection(phone_collection)

get_emp_count(name_collection)
index_data(name_collection, 'Department')
index_data(phone_collection, 'Gender')

del_emp_by_id(name_collection, 'E02003')

get_emp_count(name_collection)
search_by_column(name_collection, 'Department', 'IT')
search_by_column(name_collection, 'Gender', 'Male')
search_by_column(phone_collection, 'Department', 'IT')
get_dep_facet(name_collection)
get_dep_facet(phone_collection)
