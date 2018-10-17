from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING


def find_ancestors(bill_parents, all_bill_parents, ancestors):
    for parent in bill_parents:
        if parent not in ancestors:
            ancestors.append(parent)
            find_ancestors(all_bill_parents.get(parent, []), all_bill_parents, ancestors)
    return ancestors


def get_bill_ancestors(all_bill_parents):
    for bill_id, bill_parents in all_bill_parents.items():
        ancestors = find_ancestors(bill_parents, all_bill_parents, [])
        yield {'bill_id': bill_id, 'ancestors': ancestors}


def get_resources(resources, data):
    # key = bill_id
    # value = list of parents of this bill
    all_bill_parents = {}
    for i, resource in enumerate(resources):
        if i == data['billsplit_idx']:
            for billsplit in resource:
                # SplitBillID is the bill the MainBillID was split into
                all_bill_parents.setdefault(billsplit['SplitBillID'], set()).add(billsplit['MainBillID'])
        if i == data['billunion_idx']:
            for billunion in resource:
                # MainBillID is the bill that the UnionBillID was merged into
                all_bill_parents.setdefault(billunion['MainBillID'], set()).add(billunion['UnionBillID'])
    yield get_bill_ancestors(all_bill_parents)


def get_datapackage(datapackage, data):
    descriptors = []
    for i, descriptor in enumerate(datapackage['resources']):
        if descriptor['name'] == 'kns_billsplit':
            data['billsplit_idx'] = i
        elif descriptor['name'] == 'kns_billunion':
            data['billunion_idx'] = i
        else:
            descriptors.append(descriptor)
    datapackage['resources'] = descriptors
    fields = [{'name': 'bill_id', 'type': 'integer'},
              {'name': 'ancestors', 'type': 'array'}]
    datapackage['resources'].append({'name': 'bill_ancestors',
                                     'path': 'bill_ancestors.csv',
                                     PROP_STREAMING: True,
                                     'schema': {'fields': fields}})
    return datapackage


def main():
    parameters, datapackage, resources, data = ingest() + ({},)
    spew(get_datapackage(datapackage, data),
         get_resources(resources, data))


if __name__ == '__main__':
    main()
