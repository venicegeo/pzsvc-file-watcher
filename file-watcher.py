#!/usr/bin/env python2
# Copyright 2016, RadiantBlue Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import os
import sys
import boto3
import requests
import json
import threading
from requests_toolbelt.multipart.encoder import MultipartEncoder


class FileWatcher:

    def __init__(self, bucket, accessKey, privateKey, userName, gatewayHost):
        """Initialization logic."""
        self.bucket = bucket
        self.accessKey = accessKey
        self.privateKey = privateKey
        self.userName = userName
        self.gatewayHost = gatewayHost if gatewayHost is not None \
            else 'https://pz-gateway.stage.geointservices.io:443'

        # Create S3 Client
        self.client = boto3.client(
            's3',
            aws_access_key_id=self.accessKey,
            aws_secret_access_key=self.privateKey)

    def listen(self):
        """Continuously polls for new files in the S3 bucket."""
        def loop():
            threading.Timer(5, loop).start()
            self.scanNewFiles()
        loop()

    def scanNewFiles(self):
        """Checks if the bucket contains any new files."""
        # Loop through every object in the bucket and ingest new files.
        for item in self.client.list_objects(Bucket=self.bucket).get('Contents'):
            if self.isNewFile(item.get('Key')):
                self.ingest(item.get('Key'))

    def getPersistenceFile(self):
        """Gets a reference to the persistence file used to store names of
        previously ingested files."""
        return open('IngestedFiles.txt', 'a+')

    def isNewFile(self, fileName):
        """Checks persistence to determine if the file has been previously
        ingested or not."""
        persistenceFile = self.getPersistenceFile()
        persistenceFile.seek(0)
        found = fileName not in persistenceFile.read()
        persistenceFile.close()
        return found

    def ingest(self, fileName):
        """Sends an Ingest Job to Piazza Gateway for the S3 object."""
        # Determine the Data Type, required for the JSON Payload
        dataType = self.determineDataType(fileName)
        if dataType is None:
            return

        # Get the JSON Payload for this File
        payload = self.getIngestPayload(fileName, dataType)
        multipart_data = MultipartEncoder(fields={'body': payload})

        # Send the Request
        response = requests.post(
            '{}/job'.format(self.gatewayHost),
            data=multipart_data,
            headers={'Content-Type': multipart_data.content_type},
            auth=(os.environ.get('PZUSER'), os.environ.get('PZPASS')))
        if response.status_code is not requests.codes.created:
            print "Ingest for file {} failed with code {}. Details: {}".format(fileName, response.status_code, response.text)
        else:
            # Persist the file name, ensuring we do not Ingest it again.
            self.recordFile(fileName)
            print "Successful request for ingest of file {} of type {}".format(fileName, dataType)

    def getIngestPayload(self, fileName, dataType):
        """Gets the JSON Payload for the Gateway /job request."""
        return json.dumps(
            {
                "userName": self.userName,
                "jobType": {
                    "type": "ingest",
                    "host": False,
                    "data": {
                        "dataType": {
                            "type": dataType,
                            "location": {
                                "type": "s3",
                                "bucketName": self.bucket,
                                "fileName": fileName,
                                "domainName": "s3.amazonaws.com"
                            }
                        },
                        "metadata": {
                            "description": "Ingested automatically by FileWatcher.",
                            "classType": {
                                "classification": "unclassified"
                            }
                        }
                    }
                }
            })

    def recordFile(self, fileName):
        """Updates a record in persistence that the file has been Ingested."""
        persistenceFile = self.getPersistenceFile()
        persistenceFile.write(fileName + '\n')
        persistenceFile.close()

    def determineDataType(self, fileName):
        """Determines the type of file, based on extension. Used to populate the
        Ingest Job."""
        name, extension = os.path.splitext(fileName)
        if extension.lower() in ('.tif', '.tiff', '.geotiff', '.tfw'):
            return 'raster'
        if extension.lower() in ('.laz', '.las', '.bpf'):
            return 'pointcloud'
        if extension.lower() in ('.zip'):
            return 'shapefile'
        return None


def main():
    """Instantiates a FileWatcher based on environment variables, or command
    line args."""
    # Required inputs
    bucket, accessKey, privateKey, userName, gatewayHost = None, None, None, None, None

    # Check env vars
    bucket = os.environ.get('s3.bucket.name')
    accessKey = os.environ.get('s3.key.access')
    privateKey = os.environ.get('s3.key.private')
    userName = os.environ.get('PZUSER')
    domain = os.environ.get('DOMAIN')

    if domain:
        gatewayHost = 'https://pz-gateway.' + domain

    # Check CLI args, override env vars if present
    parser = argparse.ArgumentParser(
        description='Ingest files based on S3 uploads')
    parser.add_argument('-b', help='S3 Bucket Location')
    parser.add_argument('-a', help='S3 Access Key')
    parser.add_argument('-p', help='S3 Private Key')
    parser.add_argument('-u', help='Piazza UserName')
    parser.add_argument('-g', help='Piazza Gateway host name')
    args = parser.parse_args()

    # Assign CLI args if present
    if args.b is not None:
        bucket = args.b
    if args.a is not None:
        accessKey = args.a
    if args.p is not None:
        privateKey = args.p
    if args.u is not None:
        userName = args.u
    if args.g is not None:
        gatewayHost = args.g

    # Validate arguments
    if (bucket is None):
        print 'S3 bucket must be specified using the -b argument.'
        sys.exit(66)

    # Begin listening
    print "Listening for new files in AWS bucket {}.".format(bucket)
    print "Piazza Gateway: {}".format(gatewayHost)
    fileWatcher = FileWatcher(
        bucket=bucket,
        accessKey=accessKey,
        privateKey=privateKey,
        userName=userName,
        gatewayHost=gatewayHost)
    fileWatcher.listen()

if __name__ == "__main__":
    main()
