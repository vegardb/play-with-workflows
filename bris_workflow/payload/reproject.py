from earthkit.workflows.decorators import as_payload

@as_payload
def reproject(get_ifs, get_orography):
    return f'reprojected {get_ifs} with {get_orography}'
