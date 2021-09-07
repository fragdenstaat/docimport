# Document importer for filingcabinet from mmmeta

The `docimport.py` synchronizes a `mmmeta` database and batch-downloads documents into an output directory and places JSON meta data next to it. After every batch it calls an import command that on success marks the batch as imported and clears the output directory.

## Install

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
```

## Run


```bash
# python docimport.py <collection-slug> <s3-bucket> <s3-dir> <output-dir> <import-command>
```

Example call:
```bash
python docimport.py kleine-anfragen dokukratie-dev /var/www/dokukratie/docs/ "sudo -u fragdenstaat_de /var/www/fragdenstaat.de/scripts/manage.sh import_documents /var/www/dokukratie/docs/"
```
