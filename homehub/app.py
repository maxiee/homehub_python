import os
from pymongo import MongoClient
from minio import Minio

class Application:
    def __init__(self, app_name, mongo_uri, minio_endpoint, access_key, secret_key):
        """
        初始化应用，连接 MongoDB 和 MinIO
        
        :param app_name: 应用名称
        :param mongo_uri: MongoDB 连接 URI
        :param minio_endpoint: MinIO 连接地址
        :param access_key: MinIO 访问密钥
        :param secret_key: MinIO 访问密钥
        """
        self.app_name = app_name
        self.mongo_client = MongoClient(mongo_uri)
        self.minio_client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=True)

    def get_collection(self, collection_name):
        """获取 MongoDB 集合，集合名包含应用名称作为前缀"""
        db = self.mongo_client[self.app_name]
        return db[f"{self.app_name}_{collection_name}"]

    # MongoDB 数据操作示例
    def insert_document(self, collection_name, document):
        """插入文档到 MongoDB"""
        collection = self.get_collection(collection_name)
        return collection.insert_one(document)

    # MinIO 数据操作示例
    def upload_file(self, bucket_name, object_name, file_path):
        """上传文件到 MinIO"""
        with open(file_path, 'rb') as file_data:
            file_stat = os.stat(file_path)
            self.minio_client.put_object(bucket_name, object_name, file_data, file_stat.st_size)
