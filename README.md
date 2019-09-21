# gcloud-uploader

## Usage

Run the following commands:

```bash
python setup.py install
upload
```

Provide folder, glob pattern, and destination folder:

```bash
Local Folder:
my_folder
Glob Pattern: (leave blank for recursive all)
**/*
Google Storage Path:
my_cloud_folder
```

Files that already exist will not be overwritten.
