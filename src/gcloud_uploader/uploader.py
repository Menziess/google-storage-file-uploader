from google.cloud import storage
from dotenv import load_dotenv

import argparse
import glob
import os

load_dotenv('.env')

BUCKET = os.getenv('BUCKET')


def list_files(pattern='*'):
    """List files using glob pattern, exclude directories."""
    return (
        x for x in glob.iglob(pattern, recursive=True)
        if not os.path.isdir(x)
    )


def list_blobs(bucket_name, folder='', client=storage.Client()):
    """Lists all the blobs in the bucket."""
    for x in client.list_blobs(bucket_name):
        path = repr(x).split(',')[1].strip()
        if path.startswith(folder):
            yield path


def upload_blob(
    bucket,
    source_file_name,
    destination_blob_name
):
    """Uploads a file to the bucket."""
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def upload_blobs(
    bucket_name,
    source_folder,
    destination_folder,
    pattern='**/*'
):
    """Uploads files recursively to the bucket."""
    client = storage.Client()

    print("Connecting to blob storage...")
    bucket = client.get_bucket(bucket_name)

    # Get existing blobs to prevent reuploading
    print("Getting existing blob list...")
    existing_blobs = list(list_blobs(bucket_name, destination_folder, client))

    # # Gather filenames to be uploaded
    filenames = list_files(os.path.join(source_folder, pattern))

    # Copy files 1 to 1 from source to destination if not exitsts
    for filename in filenames:

        destination_path = os.path.join(
            destination_folder,
            os.path.relpath(filename, source_folder)
        )

        if destination_path not in existing_blobs:
            upload_blob(
                bucket,
                filename,
                destination_path
            )
        else:
            print('Already uploaded:', filename)


def get_args():
    """Get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-in-folder",
        type=str,
        help='The input folder from which files are uploaded.'
    )
    parser.add_argument(
        "-out-folder",
        type=str,
        help='The output folder on blob storage.'
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default='**/*',
        help='The glob pattern used to select files in folder.'
    )
    return parser.parse_args()


def upload():

    args = get_args()

    try:

        if not args.in_folder and not args.out_folder:
            print("Let's upload some stuff to Google Cloud.")
            in_folder = input("Local Folder:\n")
            assert os.path.isdir(in_folder), "Path doesn't exist..."
            pattern = input("Glob Pattern: (default: '**/*')\n")
            out_folder = input("Google Storage Path:\n")

        arguments = {
            'bucket_name': BUCKET,
            'source_folder': args.in_folder or in_folder,
            'destination_folder': args.out_folder or out_folder,
            'pattern': args.pattern or pattern or '**/*'
        }

        upload_blobs(**arguments)
        print("\nFinished uploading.")

    except KeyboardInterrupt:
        print("\nYou stopped the program.")


if __name__ == "__main__":
    upload()
