from earthkit.workflows.decorators import as_payload


@as_payload
def run(state):
    return f'Running BRIS with state: {state}'
