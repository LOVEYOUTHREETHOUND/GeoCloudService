from minio import Minio
from minio.error import S3Error
import src.config.config as config
from src.utils.logger import logger

# 读取 MinIO 配置
minio_host = config.MINIO_HOST
minio_port = config.MINIO_PORT
minio_access_key = config.MINIO_ACCESS_KEY
minio_secret_key = config.MINIO_SECRET_KEY
minio_secure = config.MINIO_SECURE  
bucket_name = config.MINIO_BUCKET

def create_minio_client():
    try:
        minio_url = f"{minio_host}:{minio_port}"
        client = Minio(
            minio_url,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure
        )
        logger.info("MinIO 客户端创建成功")
        return client
    except Exception as e:
        logger.error(f"创建 MinIO 客户端失败: {e}")
        return None

def check_or_create_bucket(client: Minio, bucket_name: str):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"存储桶 '{bucket_name}' 创建成功")
        else:
            logger.info(f"存储桶 '{bucket_name}' 已存在")
    except S3Error as e:
        logger.error(f"存储桶操作失败: {e}")

def upload_file(client: Minio, bucket_name: str, file_path: str, object_name: str):
    try:
        client.fput_object(bucket_name, object_name, file_path)
        logger.info(f"文件 '{file_path}' 上传为对象 '{object_name}' 成功")
    except S3Error as e:
        logger.error(f"上传文件失败: {e}, file_path: {file_path}, object_name: {object_name}")

def download_file(client: Minio, bucket_name: str, object_name: str, dest_path: str):
    try:
        client.fget_object(bucket_name, object_name, dest_path)
        logger.info(f"对象 '{object_name}' 下载到 '{dest_path}' 成功")
    except S3Error as e:
        logger.error(f"下载文件失败: {e}, object_name: {object_name}, dest_path: {dest_path}")

def list_objects(client: Minio, bucket_name: str, prefix: str = ""):
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        result = []
        for obj in objects:
            result.append({"object_name": obj.object_name, "size": obj.size})
        logger.info(f"存储桶 '{bucket_name}' 下的对象列表: {result}")
        return result
    except S3Error as e:
        logger.error(f"列举对象失败: {e}")
        return None

def delete_object(client: Minio, bucket_name: str, object_name: str):
    try:
        client.remove_object(bucket_name, object_name)
        logger.info(f"对象 '{object_name}' 删除成功")
    except S3Error as e:
        logger.error(f"删除对象失败: {e}, object_name: {object_name}")
