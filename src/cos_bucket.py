import hashlib
import os
from functools import partial
from pathlib import Path
from traceback import format_exc
from urllib.parse import quote

from loguru import logger as log
from qcloud_cos import CosServiceError

from .cos import TencentCos
from .exceptions import CosBucketDirNotFoundError

REGIONS = ['nanjing', 'chengdu', 'beijing', 'guangzhou', 'shanghai', 'chongqing', 'hongkong']


class TencentCosBucket(object):
    """腾讯云COS桶文件操作"""

    def __init__(self, cos: TencentCos, bucket_name: str):
        self.cos = cos
        self.name = bucket_name
        self.full_name = bucket_name + '-' + self.cos.get_appid()
        self.base_url = self._get_bucket_url()
        self.region = self._get_correct_cos_region()

    def _get_bucket_url(self):
        """获取存储桶URL"""
        return 'https://' + self.full_name + '.cos.' + self.cos.region + '.myqcloud.com/'

    def _get_correct_cos_region(self):
        """获取存储桶的正确地区配置"""
        for region in REGIONS:
            try:
                self.cos.client.list_objects(Bucket=self.full_name)
                return region
            except CosServiceError as e:
                if e.get_error_code() == 'NoSuchBucket':
                    log.warning(f'bucket {self.name} not in region '
                                f'{self.cos.region}, now try another region...')
                    region = f'ap-{region}'
                    self.cos = TencentCos(self.cos.secret_id, self.cos.secret_key, region)
                    self.full_name = self.name + '-' + self.cos.get_appid()
                    continue
            except Exception as e:
                log.error(f'Fatal: {str(e)}')

    def _list_objects(self, prefix: str = ''):
        """列出远程对象/文件"""
        response = self.cos.client.list_objects(Bucket=self.full_name, Prefix=prefix)
        if 'Contents' in response:
            return [c['Key'].replace(prefix, '') for c in response['Contents']]
        else:
            log.warning(f'Cannot find any objects in bucket {self.name}')
            return []

    def list_all_dirs(self):
        """列出所有远程文件夹"""
        return [ob[:-1] for ob in self._list_objects() if ob.endswith('/')]

    def list_all_files(self):
        """列出所有远程文件"""
        return [ob for ob in self._list_objects() if not ob.endswith('/')]

    def list_dir_files(self, remote_dir: str):
        """列出特定文件夹下远程文件"""
        if remote_dir not in ['', '/']:
            if remote_dir.startswith('/'):
                remote_dir = remote_dir[1:]
            if not remote_dir.endswith('/'):
                remote_dir += '/'
            if remote_dir[:-1] not in self.list_all_dirs():
                raise CosBucketDirNotFoundError(f'Bucket dir {remote_dir} not found.')
        return self._list_objects(prefix=remote_dir)

    def mkdir(self, remote_dir_path: str):
        """创建文件夹"""
        if not remote_dir_path.endswith('/'):
            remote_dir_path += '/'
        try:
            self.cos.client.put_object(
                Bucket=self.full_name,
                Body=b'',
                Key=remote_dir_path
            )
            log.success(f'Bucket {self.name} make dir {remote_dir_path} OK.')
            return True
        except Exception as e:
            log.error(f'Bucket {self.name} make dir {remote_dir_path} Failed. detail: {e}')
            return False

    def upload_file(self, local_file_path: str, remote_dir: str = '', overwrite=True):
        """上传本地文件到远程指定文件夹"""
        result, msg = self._upload_object(local_file_path, remote_dir, overwrite)
        if result is False:
            log.error(f'Upload local file: {local_file_path} Failed, detail: {msg}')
        return result

    def _upload_object(self, local_file_path: str, remote_dir: str = '', overwrite=True):
        """上传单个对象"""
        if not os.path.exists(local_file_path):
            err_msg = f'local path: {local_file_path} doesnt exists'
            log.error(err_msg)
            return False, err_msg
        p = Path(local_file_path)
        file_name = p.name

        if file_name in self._list_objects():
            if not overwrite:
                warning_msg = f'{file_name} already in {self.name}, skipped!'
                log.warning(warning_msg)
                return True, warning_msg
            else:
                log.warning(f'{file_name} already in {self.name}, overwrite!')
        try:
            with open(local_file_path, 'rb') as f:
                self.cos.client.put_object(
                    Bucket=self.full_name,
                    Body=f,
                    Key=os.path.join(remote_dir, file_name),
                    StorageClass='STANDARD',
                    EnableMD5=True,
                    Metadata={'x-cos-meta-md5': self.get_local_file_md5(local_file_path)}
                )
            log.success(f'Upload {local_file_path} to {remote_dir} Success!')
        except CosServiceError as e:
            return False, e.get_error_code()
        except Exception as e:
            return False, str(e)

        return True, 'SUCCESS'

    def download_file(self, remote_file_path: str, local_dir: str):
        """TODO: 此接口仅适合下载20MB以下的文件，待实现大文件续传"""
        return self._download_object(remote_file_path, local_dir)

    def _download_object(self, remote_file_path: str, local_dir: str):
        """下载单个对象"""
        file_dir, file_name = self._parse_object_path_name(remote_file_path)
        local_file_path = os.path.join(local_dir, file_name)
        try:
            self.cos.client.download_file(
                Bucket=self.full_name,
                Key=remote_file_path,
                DestFilePath=local_file_path
            )
            log.success(f'Download {file_name} to {local_file_path} success!')
            return True
        except Exception as e:
            log.error(f'Download {file_name} to {local_file_path} failed! '
                      f'detail: {e}, {format_exc()}')
            return False

    def _delete_object(self, object_full_path: str):
        """删除指定路径对象"""
        log.warning(f'Bucket {self.name}, delete object: {object_full_path}')
        self.cos.client.delete_object(
            Bucket=self.full_name,
            Key=object_full_path
        )

    def delete_file(self, remote_file_path: str):
        """删除远程文件"""
        if not self.is_object_exists(remote_file_path):
            log.warning(f'{remote_file_path} not in bucket {self.name}')
            return True
        else:
            remote_dir, object_key = self._parse_object_path_name(remote_file_path)
            return self._delete_file(remote_dir, object_key)

    def _delete_file(self, remote_dir, object_key):
        """删除文件夹内的单个对象"""
        if not remote_dir.endswith('/') and remote_dir != '':
            remote_dir += '/'
        if object_key not in self.list_dir_files(remote_dir):
            err_msg = f'{object_key} not found in {remote_dir}'
            log.error(err_msg)
            return False, err_msg
        else:
            self._delete_object(remote_dir + object_key)
            return True, ''

    def delete_dir_files(self, remote_dir: str):
        """删除文件夹内所有对象"""
        for file in self.list_dir_files(remote_dir):
            self._delete_object(file)

    def delete_all_files(self):
        """删除所有文件，清空存储桶"""
        for obj in self._list_objects():
            self._delete_object(obj)
        log.warning(f'Bucket {self.name} all files has been deleted')

    def is_object_exists(self, object_full_path: str):
        return self.cos.client.object_exists(
            Bucket=self.full_name,
            Key=object_full_path
        )

    def get_file_md5(self, remote_file_path: str):
        """获取远程文件的MD5哈希值"""
        if not self.is_object_exists(remote_file_path):
            log.error(f'{remote_file_path} not in bucket {self.name}')
            return ''
        else:
            return self._get_object_md5hash(remote_file_path)

    def _get_object_md5hash(self, object_full_path: str):
        """获取文件md5哈希值 https://cloud.tencent.com/document/product/436/36427"""
        response = self._get_object_info(object_full_path)
        md5hash = response.get('x-cos-meta-md5')
        log.info(f'Bucket file: {object_full_path}, md5: {md5hash}')
        return md5hash

    def _get_object_info(self, object_full_path: str):
        """获取对象元数据信息"""
        return self.cos.client.get_object(
            Bucket=self.full_name,
            Key=object_full_path
        )

    def get_file_url(self, remote_file_path: str):
        """获取远程文件的url"""
        if not self.is_object_exists(remote_file_path):
            log.error(f'{remote_file_path} not in bucket {self.name}')
            return ''
        else:
            remote_dir, object_key = self._parse_object_path_name(remote_file_path)
            return self._get_object_url(remote_dir, object_key)

    def _get_object_url(self, remote_path: str, object_key: str):
        """获取指定对象的URL"""
        object_url = self.base_url + quote(remote_path + object_key)
        log.info(f'get {remote_path + object_key} url: {object_url}')

        return object_url

    @staticmethod
    def _parse_object_path_name(object_full_path: str):
        """根据对象的全路径获取所在文件夹和文件名"""
        if '/' not in object_full_path:
            return '', object_full_path
        else:
            paths = object_full_path.split('/')
            return '/'.join(paths[:-1]), paths[-1]

    @staticmethod
    def get_local_file_md5(local_file_path: str) -> str:
        """获取文件的md5哈希值"""
        if not os.path.isfile(local_file_path):
            raise FileNotFoundError(f'cannot found file: {local_file_path}')
        with open(local_file_path, 'rb') as f:
            md5hash = hashlib.md5()
            for buffer in iter(partial(f.read, 128), b''):
                md5hash.update(buffer)
            return md5hash.hexdigest()
