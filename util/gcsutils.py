import fnmatch

class GCSUtils:
    """List all files in gcp bucket"""
    def list_files(self,storage_client, bucketname, pattern):
        filelist = []
        bucket = storage_client.bucket(bucketname)
        files = bucket.list_blobs()
        for blob in files:
            if fnmatch.fnmatch(blob.name,pattern):
                filelist.append(blob.name)
        return filelist

    def download_blob(self, storage_client, bucket_name, source_blob_name, destination_file_name, context):
        """Downloads a blob from bucket"""
        #bucket_name = "bucket-name"
        #source_blob_name = "storage-object-name"
        #destination_file_name = "local/path/to/file"
        #storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        #context.logger.info("Blob {} downloaded to {}".format(source_blob_name, destination_file_name))

    def upload_blob(self, storage_client, bucket_name, source_file_name, destination_blob_name, context):
        """uploads a blob from bucket"""
        #bucket_name = "bucket-name"
        #source_blob_name = "storage-object-name"
        #destination_file_name = "local/path/to/file"
        #storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        #context.logger.info("Blob {} uploaded to {}".format(source_blob_name, destination_file_name))

    def delete_blob(self, storage_client, bucket_name, blob_name, context):
        """Deletes a blob from bucket"""
        #bucket_name = "name"
        #blob_name = "obj-name"

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        context.logger.info("Blob {} deleted".format(blob_name))

    def copy_blob(self, storage_client, bucket_name, blob_name, destination_bucket_name, destination_blob_name, context):
        """Copies a blob from one to other"""
        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)
        blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
