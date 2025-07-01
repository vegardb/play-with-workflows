from earthkit.workflows.decorators import as_payload

@as_payload
def get():
    return 'orography'
