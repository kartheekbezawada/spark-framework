pip install azure-identity
pip install azure-keyvault-secrets


from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

class KeyVaultClient:
    def __init__(self, key_vault_name):
        self.key_vault_name = key_vault_name
        self.kv_uri = f"https://{key_vault_name}.vault.azure.net"
        self.credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=self.kv_uri, credential=self.credential)
        
    def get_secret(self, secret_name):
        retrieved_secret = self.client.get_secret(secret_name)
        return retrieved_secret.value


class SharePointClient:
    def __init__(self, site_url, username, password):
        self.site_url = site_url
        self.username = username
        self.password = password
        self.ctx = self.authenticate()

    def authenticate(self):
        return ClientContext(self.site_url).with_credentials(UserCredential(self.username, self.password))

    def get_files(self, folder_url):
        folder = self.ctx.web.get_folder_by_server_relative_url(folder_url)
        files = folder.files
        self.ctx.load(files)
        self.ctx.execute_query()
        return files

    def download_file(self, file):
        file_name = file.properties["Name"]
        file_content = file.open_binary()
        self.ctx.execute_query()
        return file_name, file_content.content


class AzureBlobClient:
    def __init__(self, connection_string, container_name):
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def upload_file(self, file_name, file_content):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_name)
        blob_client.upload_blob(file_content)
        print(f'Uploaded {file_name} to Azure Blob Storage.')


class DataTransfer:
    def __init__(self, sp_client, blob_client):
        self.sp_client = sp_client
        self.blob_client = blob_client

    def transfer_files(self, sharepoint_folder_url):
        files = self.sp_client.get_files(sharepoint_folder_url)
        for file in files:
            file_name, file_content = self.sp_client.download_file(file)
            self.blob_client.upload_file(file_name, file_content)
        print('All files have been uploaded successfully.')


# Define your Key Vault name
key_vault_name = 'your-key-vault-name'

# Create Key Vault client
kv_client = KeyVaultClient(key_vault_name)

# Retrieve secrets from Azure Key Vault
sharepoint_site_url = kv_client.get_secret('sharepoint-site-url')
sharepoint_user = kv_client.get_secret('sharepoint-username')
sharepoint_password = kv_client.get_secret('sharepoint-password')
azure_blob_connection_string = kv_client.get_secret('azure-blob-connection-string')
azure_blob_container_name = kv_client.get_secret('azure-blob-container-name')
sharepoint_folder_url = kv_client.get_secret('sharepoint-folder-url')

# Create SharePoint and Azure Blob clients
sp_client = SharePointClient(sharepoint_site_url, sharepoint_user, sharepoint_password)
blob_client = AzureBlobClient(azure_blob_connection_string, azure_blob_container_name)

# Create DataTransfer object and transfer files
data_transfer = DataTransfer(sp_client, blob_client)
data_transfer.transfer_files(sharepoint_folder_url)
