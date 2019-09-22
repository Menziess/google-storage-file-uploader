from google.cloud import storage
from dotenv import load_dotenv
from datetime import datetime
from time import sleep

import multiprocessing_logging
import multiprocessing
import argparse
import logging
import glob
import os

load_dotenv('.env')

BUCKET = os.getenv('BUCKET')
multiprocessing.set_start_method('spawn', True)
multiprocessing_logging.install_mp_handler()


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


def get_last_uploaded_blobname(bucket_name, destination_folder, client):
    """Assuming filenames are incrementally increasing, we can compare
    to the last uploaded filename on blob."""

    # Get existing blobs to prevent reuploading
    print("Getting existing blob list...")
    existing_blobs = list(list_blobs(bucket_name, destination_folder, client))

    item = None
    for x in existing_blobs:
        item = x
    return item


def upload_blob(
    bucket,
    source_file_name,
    destination_blob_name,
    log
):
    """Uploads a file to the bucket."""
    blob = bucket.blob(destination_blob_name)
    # blob.upload_from_filename(source_file_name)
    log.info(blob)
    log.info('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def worker(
    bucket,
    filename,
    destination_folder,
    source_folder,
    last_uploaded_blobname,
    log
):
    destination_path = os.path.join(
        destination_folder,
        os.path.relpath(filename, source_folder)
    )

    if destination_path > last_uploaded_blobname:
        upload_blob(
            bucket,
            filename,
            destination_path,
            log
        )
    else:
        log.info('Already uploaded:' + filename)


def upload_blobs(
    bucket_name,
    source_folder,
    destination_folder,
    pattern='**/*',
    parallelism=4
):
    """Uploads files recursively to the bucket."""
    client = storage.Client()

    print("Connecting to blob storage...")
    bucket = client.get_bucket(bucket_name)

    # # Gather filenames to be uploaded
    filenames = list_files(os.path.join(source_folder, pattern))

    # Used to determine whether next file must be uploaded
    last_uploaded_blobname = get_last_uploaded_blobname(
        bucket_name,
        destination_folder,
        client
    )

    log = logging.getLogger()

    for filename in filenames:
        p = multiprocessing.Process(
            target=worker,
            args=(
                bucket,
                filename,
                destination_folder,
                source_folder,
                last_uploaded_blobname,
                log
            )
        )
        p.start()
        p.join()


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


def retry(
    f,
    number_retries=5,
    seconds_between=5,
    reset_retries_after_seconds=200
):
    """Retry when a function fails."""

    retries = number_retries
    time_first_failure = None
    exceptions = []

    while retries:
        try:
            f()
        except Exception as e:

            # Store exception
            exceptions.append(e)

            # See if retries have to be reset because last
            # retry was pretty long ago
            if not time_first_failure:
                time_first_failure = datetime.now()
            elif (
                datetime.now() - time_first_failure
            ).total_seconds() > reset_retries_after_seconds:
                retries = number_retries

            # Handle retry logic
            retries -= 1
            time_first_failure = time_first_failure or datetime.now()
            print(str(e))

            # Wait a bit for next try
            sleep(seconds_between)

    # Print exception messages
    print('Program failed:', retries, '/', number_retries, 'left.')
    [print(str(x)) for x in exceptions]
    raise exceptions[-1]


def upload():

    args = get_args()

    try:

        if not args.in_folder or not args.out_folder:
            print("Let's upload some stuff to Google Cloud.")
            in_folder = input("Local Folder:\n")
            assert os.path.isdir(in_folder), "Path doesn't exist..."
            pattern = input("Glob Pattern: (default: '**/*')\n")
            out_folder = input("Google Storage Path:\n")
        else:
            assert os.path.isdir(args.in_folder), "Path doesn't exist..."

        arguments = {
            'bucket_name': BUCKET,
            'source_folder': args.in_folder or in_folder,
            'destination_folder': args.out_folder or out_folder,
            'pattern': args.pattern or pattern or '**/*'
        }

        def job(): return upload_blobs(**arguments)

        retry(
            job,
            number_retries=5,
            seconds_between=5,
            reset_retries_after_seconds=200
        )

        print("\nFinished uploading.")

    except KeyboardInterrupt:
        print("\nYou stopped the program.")


if __name__ == "__main__":
    upload()
