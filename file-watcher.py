import argparse
import os, sys
import boto3

class FileWatcher:

	def __init__(self, bucket, accessKey, privateKey):
		# Initialization
		self.bucket = bucket
		self.accessKey = accessKey
		self.privateKey = privateKey

		# Create S3 Client
		client = boto3.client('s3', aws_access_key_id=self.accessKey, aws_secret_access_key=self.privateKey)

	def listen(self):
		pass

	def checkNew(self):
		pass

	def ingest(self):
		pass

	def persistIngest(self):
		pass

	def parseGlobalVars(self):
		pass

def main():
	# Required inputs
	(bucket, accessKey, privateKey) = (None, None, None)

	# Check env vars
	bucket = os.environ.get('s3.bucket.name')
	accessKey = os.environ.get('s3.key.access')
	privateKey = os.environ.get('s3.key.private')

	# Check CLI args, override env vars if present
	parser = argparse.ArgumentParser(description='Ingest files based on S3 uploads')
	parser.add_argument('-b', help='S3 Bucket Location')
	parser.add_argument('-a', help='S3 Access Key')
	parser.add_argument('-p', help='S3 Private Key')
	args = parser.parse_args()
	
	# Assign CLI args if present
	if args.b is not None:
		bucket = args.b
	if args.a is not None:
		accessKey = args.a
	if args.p is not None:
		privateKey = args.p

	# Validate arguments
	if (bucket is None):
		print 'Invalid Inputs. S3 bucket must be specified.'
		sys.exit(66)

	# Begin listening
	fileWatcher = FileWatcher(bucket, privateKey, accessKey)
	fileWatcher.listen()

if __name__ == "__main__":
	main()