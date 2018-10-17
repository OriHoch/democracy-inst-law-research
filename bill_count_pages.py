from datapackage_pipelines.wrapper import ingest, spew
import logging


def update_magazine_pages(row, magazine_pages):
    page, magazine = int(row['PageNumber']), int(row['MagazineNumber'])
    magazine_pages.setdefault(magazine, {})
    magazine_pages[magazine].setdefault(page, [])
    magazine_pages[magazine][page].append(row)


def get_resource(resources):
    budget_magazine_pages = {}
    general_magazine_pages = {}
    for resource in resources:
        for row in resource:
            if row['budget_publication']:
                update_magazine_pages(row, budget_magazine_pages)
            else:
                update_magazine_pages(row, general_magazine_pages)
    for magazine_pages in [budget_magazine_pages, general_magazine_pages]:
        last_magazine, last_page = 0, 0
        for magazine in sorted(magazine_pages):
            for page in sorted(magazine_pages[magazine]):
                if last_magazine > 0 and last_page > 0:
                    last_bills = magazine_pages[last_magazine][last_page]
                    if magazine == last_magazine or magazine == last_magazine + 1:
                        last_num_pages, last_comment = page - last_page, ''
                        if last_num_pages > 0:
                            if len(last_bills) > 1:
                                last_num_pages = round(last_num_pages / len(last_bills), 1)
                                last_comment = 'more then 1 bill in page, split num pages between them'
                        else:
                            last_num_pages, last_comment = 1, 'page number restarted, might be more pages'
                    else:
                        last_num_pages, last_comment = 1, 'missing magazine, might be more pages'
                    for last_bill in last_bills:
                        last_bill.update(num_pages=last_num_pages, num_pages_comment=last_comment)
                        yield last_bill
                last_page = page
                last_magazine = magazine
        for last_bill in magazine_pages[last_magazine][last_page]:
            last_bill.update(num_pages=1, num_pages_comment='last magazine/page, might be more pages')
            yield last_bill

def main():
    parameters, datapackage, resources, stats = ingest() + ({},)
    fields = [{'name': 'num_pages', 'type': 'number'},
              {'name': 'num_pages_comment', 'type': 'string'}]
    datapackage['resources'][0]['schema']['fields'] += fields
    datapackage['resources'][0].update(name='bill_count_pages',
                                       path='bill_count_pages.csv')
    spew(datapackage, [get_resource(resources)], stats)


if __name__ == '__main__':
    main()
