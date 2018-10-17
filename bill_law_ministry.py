from datapackage_pipelines.wrapper import ingest, spew
import logging


def main():
    parameters, datapackage, resources, stats = ingest() + ({},)
    bills = {}
    israel_law_bill_ids = {}
    for bill in next(resources):
        bill['law_ministry_ids'] = []
        bills[bill['BillID']] = bill
        if bill['IsraelLawID']:
            for israel_law_id in bill['IsraelLawID']:
                israel_law_bill_ids.setdefault(israel_law_id, [])
                israel_law_bill_ids[israel_law_id].append(bill['BillID'])
    for law_ministry in next(resources):
        for bill_id in israel_law_bill_ids.get(law_ministry['IsraelLawID'], []):
            if law_ministry['GovMinistryID'] not in bills[bill_id]['law_ministry_ids']:
                bills[bill_id]['law_ministry_ids'].append(law_ministry['GovMinistryID'])
    gov_ministries = {}
    for gov_ministry in next(resources):
        gov_ministries[gov_ministry['GovMinistryID']] = gov_ministry['Name']
    for bill in bills.values():
        ministry_names = set()
        for ministry_id in bill['law_ministry_ids']:
            ministry_names.add(gov_ministries[ministry_id])
        bill['law_ministry_names'] = ', '.join(ministry_names)
    datapackage["resources"] = [datapackage['resources'][0]]
    fields = [{'name': 'law_ministry_ids', 'type': 'array'},
              {'name': 'law_ministry_names', 'type': 'string'}]
    datapackage["resources"][0]['schema']['fields'] += fields
    spew(datapackage, [bills.values()], stats)


if __name__ == '__main__':
    main()
