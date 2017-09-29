class Animal(object):
    def run(self):
        print 'Animal is running...'


class Dog(Animal):
    def run(self):
        print 'Dog is running...'


class Cat(Animal):
    def run(self):
        print 'Cat is running...'

dog = Dog()
dog.run()

cat = Cat()
cat.run()


def run_twice(animals):
    animals.run()
    animals.run()

run_twice(Animal())
run_twice(Dog())
run_twice(Cat())