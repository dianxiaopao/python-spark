# --coding:utf-8--

from student import Student
from Animal import run_twice, Animal, Dog, Cat

# students = Student('ceshi', '98.7')
# students.age = '35'
# students.getscore()
# # print students.age
# # print students.name
# # print students.score
# students.get_name()
#
#
# def run_twice(animals):
#     animals.run()
#     animals.run()
#
# run_twice(Animal())
# run_twice(Dog())
# run_twice(Cat())
#
# print dir(Dog)
# print Dog.__format__
# print hasattr(Dog, 'name')
# print hasattr(Dog, '__name__')
# print  getattr(students,'names','')


str = "ets_aoo_studneg"
print(str.upper())          # 把所有字符中的小写字母转换成大写字母
print(str.lower())          # 把所有字符中的大写字母转换成小写字母
print(str.capitalize())     # 把第一个字母转化为大写字母，其余小写
print(str.title())