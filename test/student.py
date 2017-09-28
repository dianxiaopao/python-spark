# -*-coding:utf-8-*-


class Student:
    def __init__(self, name, score):
        # 下划线开头的 是私有变量，在类外面无法访问
        self.__name = name
        self.__score = score
        self.names = name

    def getscore(self):
        print '%s的分数是：%s' % (self.__name, self.__score)

    def get_name(self):
        return self.__name

    def get_score(self):
        return self.__score
