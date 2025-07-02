from earthkit.workflows.decorators import as_payload
import earthkit.data as ekd


@as_payload
def get():
    return _get()


def _get() -> ekd.Field:
    source = ekd.from_source('file', _orography_file)
    return source[0]  # type: ignore


_orography_file = 'malawi_0_025.tif'

if __name__ == '__main__':
    _orography_file = '../../' + _orography_file
    print(_get())
