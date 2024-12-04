# pdm add "git+https://github.com/duanhongyi/gmssl.git#egg=gmssl"
import base64
import re

from gmssl.sm4 import CryptSM4, SM4_ENCRYPT, SM4_DECRYPT, PKCS7

from src.utils.logger import logger

# 封装一个工具类
class SM4Util:
    def __init__(self, key: bytes, iv: bytes = b"\x00" * 16, padding_mode=PKCS7):
        self.key = key
        self.iv = iv
        self.padding_mode = padding_mode
        self.crypt_sm4 = CryptSM4(padding_mode=self.padding_mode)

    def encrypt_ecb_base64(self, plain_text: str):
        if not plain_text:
            return None
        try:
            self.crypt_sm4.set_key(self.key, SM4_ENCRYPT)
            encrypted = self.crypt_sm4.crypt_ecb(plain_text.encode("utf-8"))
            cipher_text = base64.b64encode(encrypted).decode("utf-8")
            if cipher_text is not None and cipher_text.strip():
                cipher_text = re.sub(r"[\s\t\r\n]+", "", cipher_text)
            return cipher_text
        except Exception as e:
            logger.error("Exception occurred when encrypting data", exc_info=True)
            return None

    def decrypt_ecb_base64(self, cipher_text: str):
        if not cipher_text:
            return None
        try:
            self.crypt_sm4.set_key(self.key, SM4_DECRYPT)
            decrypted = self.crypt_sm4.crypt_ecb(base64.b64decode(cipher_text))
            return decrypted.decode("utf-8")
        except Exception as e:
            logger.error("Exception occurred when decrypting data", exc_info=True)
            return None
