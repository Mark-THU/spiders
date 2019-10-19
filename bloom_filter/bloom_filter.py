# -*- coding: utf-8 -*-
from hash_function import *
from bitarray import bitarray


class BloomFilter(object):
    def __init__(self, name, length, number, save_frequency):
        """
        :param name: 布隆过滤器名字，用于保存.txt文件
        :param length: 位数组的长度
        :param number: 使用hash函数的数量，不得超过11
        """
        self.length = length
        self.name = name
        if number < 1 or number > 11:
            print('hash函数数目不得超过11，且大于0！')
            return
        else:
            self.number = number
        self.HF = HashFunction()
        self.hash_functions = self.HF.get_hash_function(self.number)
        try:
            self.file = open(self.name, 'rb')
        except:
            self.file = open(self.name, 'wb+')
        self.bit_array = bitarray()
        self.save_frequency = save_frequency
        self.save_number = 0

        try:
            self.bit_array.fromfile(self.file)
            if self.bit_array == bitarray():
                self.file.close()
                self.bit_array = bitarray(self.length)
                self.bit_array.setall(0)
            else:
                pass
        except:
            print('无法从文件中读取信息')

    def is_contain(self, key):
        for i in range(self.number):
            location = self.hash_functions[i](key) % self.length
            if self.bit_array[location] == 0:
                return False
        return True

    def insert(self, key):
        for i in range(self.number):
            location = self.hash_functions[i](key) % self.length
            self.bit_array[location] = 1
        self.save_number = self.save_number + 1
        if self.save_number >= self.save_frequency:
            self.save_bitarray()
            self.save_number = 0

    def save_bitarray(self):
        self.file = open(self.name, 'wb')
        self.bit_array.tofile(self.file)
        self.file.close()


if __name__ == '__main__':
    bf = BloomFilter(name='test.bin', length=100000000, number=7, save_frequency=1)
    bf.insert('mark')
    print(bf.is_contain('mark'))
