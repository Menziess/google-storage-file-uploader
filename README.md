# gcloud-uploader

## Usage

First, follow this [Google Authentication section](https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication) to create a `credentials.json` file that is similar to the `credentials.example.json` file.

Modify the `.env` file so that it points to your bucket and the credentials file.

Run the following commands:

```bash
python setup.py install
upload
```

Files that already exist will not be overwritten.
