from datetime import datetime
from elasticsearch.exceptions import TransportError

def create_twitter_index(client, index='twitter'):
    create_index_body = {
      'settings': {
        # just one shard, no replicas for testing
        'number_of_shards': 1,
        'number_of_replicas': 0,
        'mappings': {
            'tweet': {
                "dynamic_templates": [{
                    "created_at_as_datetime": {
                        "match_mapping_type": "*",
                        "match": "*created_at",
                        "mapping": {
                            'type': 'date',
                            'format': "EEE MMM dd HH:mm:ss Z yyyy"
                            }
                        }
                    }
                ]}
            }
        }
    }
    # create empty index
    try:
        client.indices.create(
            index=index,
            body=create_index_body,
        )
    except TransportError as e:
        # ignore already existing index
        if e.error == 'index_already_exists_exception':
            pass
        else:
            raise
