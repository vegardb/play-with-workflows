from earthkit.workflows.decorators import as_payload

@as_payload
def get(time):
    return 'ifs@' + time
