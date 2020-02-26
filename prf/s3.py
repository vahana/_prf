import logging
import boto3
import io

from slovar import slovar
from prf.csv import CSV

log = logging.getLogger(__name__)

def includeme(config):
    Settings = slovar(config.registry.settings)
    S3.setup(Settings)


class S3(CSV):

    def __init__(self, ds, create=False):
        path = ds.ns.split('/')
        bucket_name = path[0]

        self.path = '/'.join(path[1:]+[ds.name])
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(bucket_name)

        self.file_or_buff = None
        self._total = None

    def drop_collection(self):
        for it in self.bucket.objects.filter(Prefix=self.path):
            it.delete()

    def get_file_or_buff(self):
        obj = boto3.resource('s3').Object(self.bucket.name, self.path)
        return io.BytesIO(obj.get()['Body'].read())
