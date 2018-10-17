from datapackage_pipelines.wrapper import process


def process_row(row, row_index, spec, resource_index, parameters, stats):
    if spec['name'] == parameters['resource']:
        row = {k: v for k, v in row.items() if k in parameters['fields']}
    return row


def modify_datapackage(datapackage, parameters, stats):
    for descriptor in datapackage['resources']:
        if descriptor['name'] == parameters['resource']:
            keep_fields = []
            for field in descriptor['schema']['fields']:
                if field['name'] in parameters['fields']:
                    keep_fields.append(field)
            descriptor['schema']['fields'] = keep_fields
    return datapackage


process(modify_datapackage, process_row)