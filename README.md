# opendatapy

A Python library for interacting with opendata.studio datakits.

This library contains low-level functions used by [opends-cli](https://github.com/opendatastudio/cli) to interact with opendata.studio datakits.

## Development

### Deploying to PyPI

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade build twine
python -m build  # Generate distribution archives
python -m twine upload --repository pypi dist/*  # Upload distribution archives
```
