import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError


class ContainerUtils:
    def __init__(self, connection_string):
        self.connections_string = connection_string
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def list_containers(self):
        containers = []
        try:
            all_containers = self.blob_service_client.list_containers(include_metadata=False)
            for container in all_containers:
                containers.append(container['name'])
                print(container['name'])
        except Exception as e:
            print(e)

        for c in containers:
            print(c)

    def list_container_blobs(self, container_name, sufix=''):

        container_client = self.blob_service_client.get_container_client(container_name)
        container_blobs = []
        try:
            if sufix == '':
                container_blobs = container_client.walk_blobs()
                if len(container_blobs) >0 :

                    for blob in container_blobs:
                        print(blob['name'])
                else:
                    print('No value')
            else:
                container_blobs = container_client.walk_blobs(name_starts_with=sufix)
                if len(container_blobs) >0:
                    for blob in container_blobs:
                        print(container_name, blob['name'], sep='/')
                else:
                    print('No Value')
        except ResourceNotFoundError as e:
            print(e)

        return container_blobs

    def ls(self, value=''):
        if value == '':
            self.list_containers()
        else:
            value = value.split('/')
            container = value[0]
            value.pop(0)
            sufix = '/'.join(value)
            self.list_container_blobs(container, sufix)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    conn = ContainerUtils()
    # cont = conn.list_containers()
    # for c in cont:
    #     print(c)
    blobs = conn.list_container_blobs('poaa-dev')
    for b in blobs:
        print(b)

    conn.ls()
    conn.ls('dfs')
    print()
    conn.ls('dfs/delta/AVRO_TSFHE_STG_FCST_HORIZON_JB/')