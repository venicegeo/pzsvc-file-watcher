import argparse
import os, sys
import boto3

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
		pass

	def checkIsNew(self, fileName):
		"""Checks persistence to determine if the file has been previously ingested or not."""
		pass

	def ingest(self, fileName):
		"""Sends an Ingest Job to Piazza Gateway."""
		pass

	def getIngestPayload(self, fileName):
		"""Gets the JSON Payload for the Gateway /job request."""
		pass

	def recordFile(self, fileName):
		"""Updates a record in persistence that the file has been Ingested."""
		pass

	def determineFileType(self, fileName):
		"""Determines the type of file, based on extension. Used to populate the Ingest Job."""
		pass

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