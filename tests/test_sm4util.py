import unittest
from src.utils.sm4encry import SM4Util
from src.config.config import SM4_KEY

class TestSM4Util(unittest.TestCase):
    def setUp(self):
        """设置测试环境"""
        self.sm4 = SM4Util(key=SM4_KEY)
        self.test_str = "你好，世界！🌏"
        self.expected_cipher_text = "DP3Caz72O7fsk/rmz0T3wxrCEZZXj+5cT2zN6KvnZZI="

    def test_encrypt_decrypt(self):
        """测试加密解密是否正确"""
        print("测试加密解密是否正确")
        # 测试加密
        cipher_text = self.sm4.encrypt_ecb_base64(self.test_str)
        self.assertEqual(cipher_text, self.expected_cipher_text)

        # 测试解密
        plain_text = self.sm4.decrypt_ecb_base64(cipher_text)
        self.assertEqual(plain_text, self.test_str)

if __name__ == '__main__':
    unittest.main()