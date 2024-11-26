import time

from src.utils.logger import logger
import src.config.config as config

class IdMaker:
    instance = None
    def __init__(self, datacenter_id, worker_id, sequence=0):
        """
        初始化
        :param datacenter_id: 数据中心(机器区域)ID
        :param worker_id: 机器ID
        :param sequence: 其实序号
        """
        # sanity check
        if worker_id > config.MAX_WORKER_ID or worker_id < 0:
            raise ValueError('worker_id值越界')

        if datacenter_id > config.MAX_DATACENTER_ID or datacenter_id < 0:
            raise ValueError('datacenter_id值越界')

        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence = sequence

        self.last_timestamp = -1  # 上次计算的时间戳

    @classmethod
    def get_instance(cls):
        # 如果实例不存在，则创建一个新的实例
        if cls.instance is None:
            cls.instance = cls(1, 1, 0)
        return cls.instance
        
    def _gen_timestamp(self):
        """
        生成整数时间戳
        :return:int timestamp
        """
        return int(time.time() * 1000)

    def get_id(self):
        """
        获取新ID
        :return:
        """
        timestamp = self._gen_timestamp()

        # 时钟回拨
        if timestamp < self.last_timestamp:
            logger.error('clock is moving backwards. Rejecting requests until {}'.format(self.last_timestamp))
            raise

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & config.SEQUENCE_MASK
            if self.sequence == 0:
                timestamp = self._til_next_millis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        new_id = ((timestamp - config.TWEPOCH) << config.TIMESTAMP_LEFT_SHIFT) | (self.datacenter_id << config.DATACENTER_ID_SHIFT) | \
                 (self.worker_id << config.WOKER_ID_SHIFT) | self.sequence
        return new_id

    def _til_next_millis(self, last_timestamp):
        """
        等到下一毫秒
        """
        timestamp = self._gen_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._gen_timestamp()
        return timestamp
    
def getPkId():
    id_maker = IdMaker.get_instance()
    return id_maker.get_id()
