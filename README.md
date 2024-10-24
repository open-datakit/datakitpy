# datakitpy

This library contains low-level functions used by [opendatacli](https://github.com/open-datakit/cli) to interact with datakits.

## Development

### Deploying to PyPI

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade build twine
python -m build  # Generate distribution archives
python -m twine upload --repository pypi dist/*  # Upload distribution archives
```
