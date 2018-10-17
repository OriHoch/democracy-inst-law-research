from datapackage_pipelines.wrapper import ingest, spew
import logging


TAZKIR_NAME_PREFIXES = ['צו',
                        'חוק ה', 'תזכיר', 'תקנות', 'תזכיר ל', 'הצעת חוק',
                        'טיוטת צו', 'תזכיר צו', 'תזכיר חוק',
                        'תזכיר חוק', 'טיוטת צו ה', 'טיוטת צו ל', 'תזכיר  חוק',
                        'תזכיר חוק ה', 'טיוטת חוק ל',
                        'טיוטת תקנות', 'תזכיר חוק ה', 'תזכיר חוק ל', 'תזכיר טיוטת',
                        'תזכיר פקודת', 'תזכיר תקנות',
                        'טיוטת תקנות:', 'תזכיר  חוק ל', 'תזכיר חוק  ה', 'תזכיר חוק  ל',
                        'טיוטת תקנות ה',
                        'טיוטת תיקון ה', 'טיוטת תקנות ה', 'תזכיר חוק יסוד', 'טיוטת  תקנות  -',
                        'תזכיר חוק יסוד:',
                        'תזכיר תיקון חוק', 'תזכירחוק לתיקון', 'תזכירי חוק יסוד', 'תזכיר חוק לתיקון',
                        'טרום תזכיר חוק ה',
                        'תזכיר הצעת חוק ה', 'תזכיר חוק- יסוד:', 'טיוטת  תקנות  צו ל',
                        'טיוטת תקנות - צו ה',
                        'טיוטת תקנות - צו  ל', 'דברי ההסבר לתזכיר חוק']


HEB_DATE_PREFIXES = ['התשע"',
                     'התשסע"',
                     'התשעה',
                     'התשס"',
                     "התשס''",
                     'התש"ע',
                     "התשע''",
                     'תשע"',
                     'התשע']


def get_resources(resources):
    tazkir_name_office = {}
    for tazkir in next(resources):
        name = tazkir['name'].strip()
        # logging.info(name)
        for prefix in sorted(TAZKIR_NAME_PREFIXES, key=lambda k: len(k), reverse=True):
            if name.startswith(prefix):
                name = name.replace(prefix, '').strip()
                break
        datepart = None
        for prefix in HEB_DATE_PREFIXES:
            if prefix in name:
                name, datepart = name.split(prefix)
                break
        if not datepart:
            logging.warning('failed to get datepart for {}'.format(name))
        name = name.strip(', ')
        if len(name) > 7:
            tazkir_name_office[name] = tazkir['office']
    for bill in next(resources):
        tazkir_offices = set()
        for tazkir_name, office in tazkir_name_office.items():
            if tazkir_name in bill['Name']:
                tazkir_offices.add(office)
        bill['tazkir_offices'] = ', '.join(tazkir_offices)
        yield bill


def main():
    parameters, datapackage, resources, stats = ingest() + ({},)
    get_resources(resources)
    datapackage["resources"] = [datapackage['resources'][1]]
    datapackage['resources'][0].update(name='bill_tazkirim',
                                       path='bill_tazkirim.csv')
    fields = [{'name': 'tazkir_offices', 'type': 'string'}]
    datapackage['resources'][0]['schema']['fields'] += fields
    spew(datapackage, [get_resources(resources)], stats)


if __name__ == '__main__':
    main()
