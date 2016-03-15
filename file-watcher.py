import argparse
import os, sys
import boto3
import atexit

class FileWatcher:

	def __init__(self, bucket, accessKey, privateKey, pzApiKey):
		"""Initialization logic."""
		self.bucket = bucket
		self.accessKey = accessKey
		self.privateKey = privateKey
		self.pzApiKey = pzApiKey

		# Create S3 Client
		self.client = boto3.client('s3', aws_access_key_id=self.accessKey, aws_secret_access_key=self.privateKey)

		# Ensure the Persistence file exists. Create if not.
		self.persistenceFile = open('IngestedFiles.txt', 'a+')

	def listen(self):
		"""Polls for new files in the S3 bucket."""
		self.scanNewFiles()

	def scanNewFiles(self):
		"""Checks if the bucket contains any new files."""
		# Loop through every object in the bucket and ingest new files.
		for item in self.client.list_objects(Bucket=self.bucket).get('Contents'):
			if self.isNewFile(item.get('Key')):
				self.ingest(item.get('Key'))

	def isNewFile(self, fileName):
		"""Checks persistence to determine if the file has been previously ingested or not."""
		self.persistenceFile.seek(0)
		return not fileName in self.persistenceFile.read()

	def ingest(self, fileName):
		"""Sends an Ingest Job to Piazza Gateway for the S3 object."""
		# Determine the Data Type, required for the JSON Payload
		dataType = self.determineDataType(fileName)
		if dataType is None:
			return

		# Get the JSON Payload for this File
		payload = self.getIngestPayload(fileName, dataType)

		# Send the Request


	def getIngestPayload(self, fileName, dataType):
		"""Gets the JSON Payload for the Gateway /job request."""
		return """
			{
				"apiKey": "{}",
				"jobType": {
					"type": "ingest",
					"host": "{}",
					"data": {
						"dataType": {
							"type": "{}",
							"location": {
								"type": "s3",
								"bucketName": "{}",
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
			}
		""".format(self.pzApiKey, False, dataType, self.bucket)

	def recordFile(self, fileName):
		"""Updates a record in persistence that the file has been Ingested."""
		pass

	def determineDataType(self, fileName):
		"""Determines the type of file, based on extension. Used to populate the Ingest Job."""
		name, extension = os.path.splitext(fileName)
		if extension.lower() in ('tif', 'tiff', 'geotiff', 'tfw'):
			return 'raster'
		if extension.lower() in ('laz', 'las', 'bpf'):
			return 'pointcloud'
		return None

	def destroy(self):
		"""Cleans up persistence file resources upon termination."""
		self.persistenceFile.close()

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

	# When application terminates, ensure FileWatcher persistence buffer is flushed 
	atexit.register(fileWatcher.destroy)

if __name__ == "__main__":
	main()