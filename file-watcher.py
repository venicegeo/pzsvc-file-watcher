import argparse
import os, sys

class FileWatcher:
	(bucket, accessKey, privateKey) = (None, None, None)

	def __init__(self, bucket, accessKey, privateKey):
		self.bucket = bucket
		self.accessKey = accessKey
		self.privateKey = privateKey

	def listen(self):
		pass

	def checkNew(self):
		pass

	def ingest(self):
		pass

	def parseGlobalVars(self):
		pass

def main():
	# Required Inputs
	(bucket, accessKey, privateKey) = (None, None, None)

	# Check Env Vars
	bucket = os.environ.get('s3.bucket.name')
	accessKey = os.environ.get('s3.key.access')
	privateKey = os.environ.get('s3.key.private')

	# Check CLI Args, Override Env Vars if present
	parser = argparse.ArgumentParser(description='Ingest files based on S3 uploads')
	parser.add_argument('-b', help='S3 Bucket Location')
	parser.add_argument('-a', help='S3 Access Key')
	parser.add_argument('-p', help='S3 Private Key')
	args = parser.parse_args()
	
	# Assign if present
	if args.b is not None:
		bucket = args.b
	if args.a is not None:
		accessKey = args.a
	if args.p is not None:
		privateKey = args.p

	# Validate Arguments
	if (bucket is None) or (accessKey is None) or (privateKey is None):
		print 'Invalid Inputs. S3 Bucket Name, Access Key, and Private Key must be specified.'
		sys.exit(66)

	# Begin Listening
	fileWatcher = FileWatcher(bucket, privateKey, accessKey)
	fileWatcher.listen()

if __name__ == "__main__":
	main()