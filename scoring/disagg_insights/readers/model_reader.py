import tempfile

import h5py
from oci.object_storage import ObjectStorageClient
from tensorflow.keras.models import load_model

from disagg_insights.constants import TENANT_NAME


class ModelReader:

    def __init__(self, config: dict, bucket_name: str, object_name: str):
        self._config = config
        self._bucket_name = bucket_name
        self._object_name = object_name
        self._os_client = ObjectStorageClient(config)

    def read(self) -> bytes:
        response = self._os_client.get_object(bucket_name=self._bucket_name,
                                              namespace_name=self._config[TENANT_NAME],
                                              object_name=self._object_name)
        return response.data.content

    @staticmethod
    def load_model_api(model_in_bytes: bytes):
        temp = tempfile.TemporaryFile()
        temp.write(model_in_bytes)
        with h5py.File(temp, 'r') as h5file:
            model = load_model(h5file)
            return model
