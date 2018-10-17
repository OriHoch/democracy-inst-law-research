import requests
from pyquery import PyQuery as pq
from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import logging
import time


URL_TEMPLATE= 'http://www.tazkirim.gov.il/WebResources/Handlers/SearchResult.ashx' \
              '?id=ctl00_ctl14_g_1d88601d_0516_4340_8e48_b14d34c52b59_ctl07' \
              '&searchtypeof=archive&weburl=/&listname=Tazkirim_Archive_2010' \
              '&freetext=&officename=&fromdate1=&todate1=&fromdate2=&todate2=' \
              '&currpage={}' \
              '&currgroup=1&order=desc&sortfield=publishDate&pages=10&groups=10'


def get_tazkir_field(row, class_name):
    if class_name == 'files':
        return row.find_class('files')[0].find('a').attrib['href']
    else:
        return row.find_class(class_name)[0].text_content()


def get_tazkir(row):
    return {class_name: get_tazkir_field(row, class_name) for class_name in ['office', 'tazkir', 'files', 'date']}


def get_page(page_num, retry_num=0):
    tazkirim = []
    url = URL_TEMPLATE.format(page_num)
    res = requests.get(url)
    assert res.status_code == 200
    page = pq(res.text)
    for row in page('.resultTableRow'):
        tazkirim.append(get_tazkir(row))
    if len(tazkirim) == 0:
        logging.info(res.text)
        logging.warning('received no tazkirim for page {}'.format(page_num))
        if retry_num < 3:
            retry_num += 1
            logging.warning('retry {}/3 (after 60 seconds sleep)'.format(retry_num))
            time.sleep(60)
            return get_page(page_num, retry_num)
    return tazkirim


def get_tazkirim(page_from, page_to):
    for page_num in range(page_from, page_to+1):
        tazkirim = get_page(page_num)
        if len(tazkirim) == 0:
            logging.warning('0 tazkirim for page {}, stopping'.format(page_num))
            break
        else:
            for tazkir in tazkirim:
                yield tazkir
            time.sleep(10)


def get_resource(parameters):
    for tazkir in get_tazkirim(parameters['page_from'], parameters['page_to']):
        yield {'office': tazkir['office'], 'name': tazkir['tazkir'],
               'file': tazkir['files'], 'date': tazkir['date']}


def main():
    parameters, datapackage, resources, stats = ingest() + ({},)
    datapackage["resources"] = [{PROP_STREAMING: True, 'name': 'tazkirim', 'path': 'tazkirim.csv', 'schema': {
                                'fields': [{'name': 'office', 'type': 'string'}, {'name': 'name', 'type': 'string'},
                                           {'name': 'file', 'type': 'string'}, {'name': 'date', 'type': 'string'}]}}]
    spew(datapackage, [get_resource(parameters)], stats)


if __name__ == '__main__':
    main()
