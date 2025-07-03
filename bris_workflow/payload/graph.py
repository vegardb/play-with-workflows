from earthkit.workflows.decorators import as_payload
import torch

_CHECKPOINT_FILE = './cloudy-skies.ckpt'

@as_payload
def create():
    return _CHECKPOINT_FILE
    # return _create()

def _create():
    ckpt = torch.load(_CHECKPOINT_FILE, weights_only=False, map_location=torch.device('cpu'))
    return ckpt

if __name__ == "__main__":
    ckpt = _create()
    print(ckpt.__class__)
