from earthkit.workflows.decorators import as_payload

@as_payload
def prepare(global_data, local_data, graph):
    return f'prepared state with {global_data}, {local_data}, and {graph}'
