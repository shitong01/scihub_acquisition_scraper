import elasticsearch
from hysds.celery import app

es_url = app.conf["GRQ_ES_URL"]
# _index = None
# _type = None

ES = elasticsearch.Elasticsearch(es_url)


def deprecate_document(index, _id):
    """
    Update the ES document with new information
    :param index: name of elasticsearch index
    :param _id: id of product delivered to ASF
    :param delivery_time: delivery time to ASF to stamp to delivered product (can we delete this note)
    :param ingest_time: ingestion time to ASF to stamp to delivered product (can we delete this note)
    :param delivery_status: status of delivery to stamp to delivered product (can we delete this note)
    :param product_tagging:
    :return:
    """
    '''

    Note: borrowed from user_tags
    @param product_id - 
    @param delivery_time - 
    '''

    new_doc = dict()
    doc = dict()
    metadata = dict()
    metadata["tags"] = "deprecated"
    doc["metadata"] = metadata
    new_doc["doc"] = doc

    ES.update(index=index, doc_type="acquisition-S1-IW_SLC", id=_id, body=new_doc)
    return True


if __name__ == "__main__":
    '''
    Main program that find IPF version for acquisition
    '''
    # txt = open("deprecate_acq.txt", "r")
    # for acq in txt:
    #     acq_id = acq.strip()
    #     update_document(_id=acq_id)
