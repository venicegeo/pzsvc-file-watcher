import argparse
import os, sys
import boto3
import atexit
import requests
import json
from requests_toolbelt.multipart.encoder import MultipartEncoder

class FileWatcher:

	def __init__(self, bucket, accessKey, privateKey, pzApiKey):
		"""Initialization logic."""
		self.bucket = bucket
		self.accessKey = accessKey
		self.privateKey = privateKey
		self.pzApiKey = pzApiKey

		# Create S3 Client
		self.client = boto3.client('s3', aws_access_key_id=self.accessKey, aws_secret_access_key=self.privateKey)

	def listen(self):
		"""Polls for new files in the S3 bucket."""
		self.scanNewFiles()

	def scanNewFiles(self):
		"""Checks if the bucket contains any new files."""
		# Loop through every object in the bucket and ingest new files.
		for item in self.client.list_objects(Bucket=self.bucket).get('Contents'):
			if self.isNewFile(item.get('Key')):
				self.ingest(item.get('Key'))

	def getPersistenceFile(self):
		"""Gets a reference to the persistence file used to store names of previously ingested files."""
		return open('IngestedFiles.txt', 'a+')

	def isNewFile(self, fileName):
		"""Checks persistence to determine if the file has been previously ingested or not."""
		persistenceFile = self.getPersistenceFile()
		found = not fileName in persistenceFile.read()
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
		response = requests.post('http://pz-gateway.cf.piazzageo.io/job', data={'body':payload })
		if response.status_code is not requests.codes.created:
			print "Ingest for file {} failed with code {}. Details: {}".format(fileName, response.status_code, response.text)
		else:
			# Persist the file name, ensuring we do not Ingest it again.
			self.recordFile(fileName)
			print "Successful ingest of file {} of type {}".format(fileName, dataType)


	def getIngestPayload(self, fileName, dataType):
		"""Gets the JSON Payload for the Gateway /job request."""
		return json.dumps(
			{
				"apiKey": self.pzApiKey,
				"jobType": {
					"type": "ingest",
					"host": False,
					"data": {
						"dataType": {
							"type": dataType,
							"location": {
								"type": "s3",
								"bucketName": self.bucket,
								"fileName": "elevation.tif",
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
		"""Determines the type of file, based on extension. Used to populate the Ingest Job."""
		name, extension = os.path.splitext(fileName)
		if extension.lower() in ('.tif', '.tiff', '.geotiff', '.tfw'):
			return 'raster'
		if extension.lower() in ('.laz', '.las', '.bpf'):
			return 'pointcloud'
		return None

def main():
	"""Instantiates a FileWatcher based on environment variables, or command line args."""
	# Required inputs
	(bucket, accessKey, privateKey, pzApiKey) = (None, None, None, None)

	# Check env vars
	bucket = os.environ.get('s3.bucket.name')
	accessKey = os.environ.get('s3.key.access')
	privateKey = os.environ.get('s3.key.private')
	pzApiKey = os.environ.get('pz.api.key')

	# Check CLI args, override env vars if present
	parser = argparse.ArgumentParser(description='Ingest files based on S3 uploads')
	parser.add_argument('-b', help='S3 Bucket Location')
	parser.add_argument('-a', help='S3 Access Key')
	parser.add_argument('-p', help='S3 Private Key')
	parser.add_argument('-pz', help='Piazza API Key')
	args = parser.parse_args()
	
	# Assign CLI args if present
	if args.b is not None:
		bucket = args.b
	if args.a is not None:
		accessKey = args.a
	if args.p is not None:
		privateKey = args.p
	if args.pz is not None:
		pzApiKey = args.pz

	# Validate arguments
	if (bucket is None):
		print 'Invalid Inputs. S3 bucket must be specified.'
		sys.exit(66)

	# Begin listening
	fileWatcher = FileWatcher(bucket, accessKey, privateKey, pzApiKey)
	fileWatcher.listen()

if __name__ == "__main__":
	main()