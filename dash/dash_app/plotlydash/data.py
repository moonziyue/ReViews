"""Prepare data for Plotly Dash."""
import pandas as pd

from elasticsearch import Elasticsearch
import psycopg2
es = Elasticsearch('ec2-3-87-157-15.compute-1.amazonaws.com:9200')

def connect_to_postgres():
    conn = psycopg2.connect(
        host='ec2-3-87-157-15.compute-1.amazonaws.com',
        port=5431,
        database='amazon_reviews',
        user='psql_usr',
        password='postgresql0939')
    cursor = conn.cursor()
    return cursor

def create_dataframe(search_input=None):
    if not search_input:
        body={'query': {'match': {'product_title': 'Amazon'}}, 'size': 10}
    else:
        if '+' in search_input:
            search_input=search_input.split('+')
            body={'query':{'bool':\
                           {'must': [{\
                                     'match':{'product_title': search_input[0]}
                                     }],\
                            'should':[{\
                                     'match':{'review_body': search_input[1]}
                                     }]\
                           }\
                          },\
                  'size':10\
                 }
        else:
            body={'query': {'match': {'product_title': search_input}}, 'size': 10}
    search_results=es.search(index='reviews', body=body)

    search_list=[]
    for i in range(10):
        search_list.append(search_results['hits']['hits'][i]['_source'])
    
    reviews_df = pd.DataFrame(search_list) 
    reviews_df['review_date']=pd.to_datetime(reviews_df['review_date'],unit='ms')
    reviews_df['review_date']=reviews_df['review_date'].dt.strftime('%Y-%m-%d')
    reviews_df.loc[reviews_df['verified_purchase']==True,'verified_purchase']='✔️'
    reviews_df.loc[reviews_df['verified_purchase']==False,'verified_purchase']=''
    reviews_df['review_body'] = reviews_df['review_body'].map(lambda x: x.replace('<br />',''))
    reviews_df['review_body'] = reviews_df['review_body'].map(lambda x: x.replace('&#34;',''))
    return reviews_df

def get_customer_info(customer_id):
    cursor=connect_to_postgres()
    query="SELECT * FROM customer WHERE customer_id='"+customer_id+"'"
    cursor.execute(query)
    records = cursor.fetchall()
    return records

def get_product_info(product_id):
    cursor=connect_to_postgres()
    query="SELECT * FROM product WHERE product_id='"+product_id+"'"
    cursor.execute(query)
    records = cursor.fetchall()
    df=pd.DataFrame(records, columns=['product_id','review_date','avg_rating','total'])
    df=df.drop(columns=['product_id'])
    df.sort_values(by=['review_date'], inplace=True)
    return df







