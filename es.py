from elasticsearch import Elasticsearch
import argparse

# ELASTIC_PASSWORD = "elastic2023"

#http://34.143.255.36:9200

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('server', type=str, help='elasticsearch server')
    parser.add_argument('user', type=str, help='elasticsearch user')
    parser.add_argument('password', type=str, help='password of elasticsearch user')
    # parser.add_argument('--kafka', type=str, nargs=2, default='null', help="import to kafka server IP:PORT TOPIC_NAME")
    args = parser.parse_args()

    client = Elasticsearch(args.server, basic_auth=(args.user, args.password))

    print(client.info())
    print(client.indices.get_alias(index="*"))
    # print("______________________________________")
    # request = '''{"query": {"match_all": {}}}'''
    # client.delete_by_query(index="it4043e-group10-processed-continent", body={"query": {"match_all": {}}})
    # print(client.search(index=".kibana", body = {
    # 'size' : 100,
    # 'query': {
    #     'match_all' : {}
    # }
    # }))