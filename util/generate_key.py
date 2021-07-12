from util.fileutils import FileUtils

class GenerateKey():
    def getfromGCS(self, context):
        pass


    def get_sftp_key(self, context):
        from google.cloud import kms
        import json
        client = kms.KeyManagementServiceClient()
        project_id=context.settings.get('KMS_PROJECT_ID')
        location_id=context.settings.get('Location_ID')
        key_ring_id=context.settings.get('google_project_id')
        key_id=context.settings.get('KEY_ID')

        key_name=client.crypto_key_path(project_id, location_id, key_ring_id, key_id)

        with open('/tmp/sftp', 'rb') as file: ciphertext = file.read()

        decrypt_response=client.decrypt(request={'name' : key_name, 'ciphertext': ciphertext})
        decrypt_response=json.loads(decrypt_response.plaintext)
        context.logger.info(decrypt_response)
        FileUtils.removeFile('/tmp/sftp')
        return(decrypt_response['password'])

    def get_API_key(self, context):
        from google.cloud import kms
        import json
        client = kms.KeyManagementServiceClient()
        project_id=context.settings.get('KMS_PROJECT_ID')
        location_id=context.settings.get('Location_ID')
        key_ring_id=context.settings.get('google_project_id')
        key_id=context.settings.get('KEY_ID')

        key_name=client.crypto_key_path(project_id, location_id, key_ring_id, key_id)

        with open('/tmp/sftp', 'rb') as file: ciphertext = file.read()

        decrypt_response=client.decrypt(request={'name' : key_name, 'ciphertext': ciphertext})
        decrypt_response=json.loads(decrypt_response.plaintext)
        context.logger.info(decrypt_response)
        FileUtils.removeFile('/tmp/api')
        return(decrypt_response['token'])