# --coding:utf-8--

from student import Student
from Animal import run_twice, Animal, Dog, Cat

students = Student('ceshi', '98.7')
students.age = '35'
students.getscore()
# print students.age
# print students.name
# print students.score
students.get_name()


def run_twice(animals):
    animals.run()
    animals.run()

run_twice(Animal())
run_twice(Dog())
run_twice(Cat())

print dir(Dog)
print Dog.__format__
print hasattr(Dog, 'name')
print hasattr(Dog, '__name__')
print  getattr(students,'names','')