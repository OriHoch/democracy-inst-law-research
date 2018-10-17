from datapackage_pipelines.wrapper import ingest, spew
import logging


def main():
    parameters, datapackage, resources, stats = ingest() + ({},)
    datapackage["resources"] = [datapackage['resources'][0]]
    datapackage["resources"][0]['schema']['fields'] += [{'name': 'classifications', 'type': 'array'},
                                                        {'name': 'classification_budget', 'type': 'boolean'},
                                                        {'name': 'classification_hesderim', 'type': 'boolean'},
                                                        {'name': 'num_pages', 'type': 'integer'},
                                                        {'name': 'is_last_page', 'type': 'boolean'}]

    def get_resource():
        bills = {}
        israel_laws = {}
        magazines = {}
        for row in next(resources):
            bills[row['BillID']] = row
            if row['IsraelLawID']:
                for israel_law_id in row['IsraelLawID']:
                    israel_laws.setdefault(israel_law_id, set()).add(row['BillID'])
            if row['MagazineNumber'] and row['PageNumber']:
                magazines.setdefault(row['MagazineNumber'], {}).setdefault(row['PageNumber'], set()).add(row['BillID'])
        for magazine_number in sorted(magazines):
            magazine = magazines[magazine_number]
            prev_page_number = None
            prev_page_bill_ids = None
            for page_number in sorted(magazine):
                if prev_page_bill_ids:
                    for bill_id in prev_page_bill_ids:
                        bills[bill_id].update(num_pages=page_number - prev_page_number, is_last_page=False)
                prev_page_number = page_number
                prev_page_bill_ids = magazine[page_number]
            for bill_id in prev_page_bill_ids:
                bills[bill_id].update(num_pages=None, is_last_page=True)
        for row in next(resources):
            if row['IsraelLawID'] in israel_laws:
                for bill_id in israel_laws[row['IsraelLawID']]:
                    bills[bill_id].setdefault('classifications', set()).add(row['ClassificiationDesc'])
        for bill in bills.values():
            classifications = list(bill.get('classifications', []))
            bill.update(classifications=classifications,
                        classification_budget='תקציב' in classifications,
                        classification_hesderim='חוקי הסדרים' in classifications)
            yield bill

    spew(datapackage, [get_resource()], stats)


if __name__ == '__main__':
    main()
