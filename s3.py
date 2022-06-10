import boto3
from boto3.s3.transfer import TransferConfig


# импорт библиотек

class S3:

    def __new__(cls):  # создаётся экземпляр класса
        if not hasattr(cls, 'instance'):
            cls.instance = super(S3, cls).__new__(cls)
        return cls.instance  # шаблон singleton

    def __init__(self):  # инициализация класса
        self.s3 = boto3.resource('s3', endpoint_url='https://s3.yandexcloud.net')  # получение ресурсов с диска

    def print_buckets_list(self):  # печать списка сегментов
        for bucket in self.s3.buckets.all():  # цикл по всем bucket
            print("Bucket name:", bucket.name)  # печать названия сегмента

    def list_objects(self, bucket, prefix):
        bucket = self.s3.Bucket(bucket)
        result = []  # создание списка
        objs = bucket.objects.filter(Prefix=prefix)  # присваивание объекту значения из сегмента по фильтру
        for obj in objs:  # цикл по всем obj
            result.append(obj)  # добавление в result obj
        return result  # вернуть result

    def get_content(self, bucket, key, asString=True):  # Функция по получения контента
        obj = self.s3.Object(bucket, key)  # присвоение obj объекта s3 с сегментом и ключом
        data = obj.get()['Body'].read()  # присовение data  данных из obj
        if (asString):
            return data.decode('utf-8')  # перевод data  в utf-8 и возврат
        else:
            return data  # возврат data

    def set_content(self, bucket, key, content):
        new_s3_object = self.s3.Object(bucket, key)  # new_s3_object = объект по по сегменту и ключу
        new_s3_object.put(Body=content)  # HTTP запрос

    def upload_file(self, bucket, key, filename):
        s3_client = boto3.client('s3', endpoint_url='https://s3.yandexcloud.net')  # создать клиента

        config = TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=10,
            multipart_chunksize=1024 * 25,
            use_threads=True
        )  # трансформация конфигурации

        s3_client.upload_file(
            filename, bucket, key,
            ExtraArgs={'ContentType': 'video/mp4'},
            Config=config
        )  # загрузка файла с именем, сегментом, ключем и конфигом
