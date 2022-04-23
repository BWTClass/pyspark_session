

class Batch:
    def __init__(self,id):
        self.id = id

    def printval(self):
        print(self.id)


if __name__ == '__main__':
    cls = Batch(id=1)
    print(cls)
    print(cls.printval())
