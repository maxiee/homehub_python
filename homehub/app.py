import os
from pymongo import MongoClient
from minio import Minio


class Application:
    def __init__(self, db_name, app_name, mongo_uri, minio_endpoint, access_key, secret_key, secure=True):
        """
        初始化应用，连接 MongoDB 和 MinIO
        
        :param app_name: 应用名称
        :param mongo_uri: MongoDB 连接 URI
        :param minio_endpoint: MinIO 连接地址
        :param access_key: MinIO 访问密钥
        :param secret_key: MinIO 访问密钥
        """
        self.db_name = db_name
        self.app_name = app_name
        self.mongo_client = MongoClient(mongo_uri)
        self.minio_client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def get_collection(self, collection_name):
        """获取 MongoDB 集合，集合名包含应用名称作为前缀"""
        # 举例，获取 mongodb://100.117.209.140:27017/ray_memx 下的 collection_name 集合
        return self.mongo_client[self.db_name][self.app_name + '_' + collection_name]
    
    def get_documents(self, collection_name, offset=0, limit=None):
        """
        获取 MongoDB 集合中的文档，支持分页

        :param collection_name: 集合名称
        :param offset: 跳过文档的数量，用于分页
        :param limit: 返回的文档数量上限
        :return: 包含文档的列表
        """
        collection = self.get_collection(collection_name)
        if limit is not None:
            return list(collection.find().skip(offset).limit(limit))
        else:
            return list(collection.find().skip(offset))

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
