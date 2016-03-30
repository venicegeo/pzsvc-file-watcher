## Piazza File Watcher

This script, when pointed to a S3 bucket, will continously scan the bucket for new files that [Piazza](https://github.com/venicegeo/venice/wiki/PiazzaCoreServices) is capable of ingesting; and when found, will create an [Ingest Job](https://github.com/venicegeo/venice/wiki/Pz-Ingest#example-ingest-requests) that will request the data be loaded into Piazza.

## Installation

Install required Python dependencies using [pip](https://pypi.python.org/pypi/pip).

```
pip install -r requirements.txt
```

## Configuration

There are two ways to configure the file-watcher script to connect to your S3 bucket. The first, recommended, way is to use environment variables. The second way is to pass configuration in directly as command line arguments. When present, command line arguments will always override the environment variables.

### Environment Variables

> * *s3.bucket.name*: The name of the S3 bucket to watch
> * *s3.key.access*: The S3 access key, if required, to connect to the S3 bucket
> * *s3.key.private*: The S3 private key, if required, to connect to the S3 bucket
> * *pz.api.key*: The Piazza [API key](https://github.com/venicegeo/venice/wiki/Pz-Gateway#authentication-and-authorization).

### Command Line Arguments

To get a list of the command line arguments that the file-watcher accepts, enter:

```
python file-watcher.py --help
```

> * *-b*: The name of the S3 bucket to watch
> * *-a*: The S3 access key, if required, to connect to the S3 bucket
> * *-p*: The S3 private key, if required, to connect to the S3 bucket
> * *-pz*: The Piazza [API key](https://github.com/venicegeo/venice/wiki/Pz-Gateway#authentication-and-authorization).
> * *-g*: The endpoint of the Gateway. Defaults to `pz-gateway.cf.piazzageo.io`.

## Example Usage

Listen to an open S3 bucket:

```
python file-watcher.py -b BUCKET_NAME -a[AWS Access Key ID] -p[AWS Secret Access Key]
```

Test locally with a debug version of the Gateway:

```
python file-watcher.py -b BUCKET_NAME -g http://localhost:8081 -a[AWS Access Key ID] -p[AWS Secret Access Key]
```

## Persistence

For simplistic persistence, this file-watcher script uses a text file on disk to track the files that it has successfully ingested to the uploader. This ensures no duplicate ingests occur. The file created will be named `IngestedFiles.txt` and will reside in the same relative directory as the `file-watcher.py` script.
