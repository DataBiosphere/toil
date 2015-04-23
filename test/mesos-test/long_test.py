from time import sleep
from jobTree.scriptTree.target import Target


class long_test(Target):
    def __init__(self):
        Target.__init__(self, time=1, memory=1000000, cpu=1)

    def run(self):
        self.addChildTarget(self.hello_world())
        self.setFollowOnTarget(self.hello_world_child())

    def hello_world(self):
        with open ('hello_world_child.txt', 'w') as file:
            file.write('This is a triumph')

    def hello_world_child(self):
        sleep(10)
        with open ('hello_world_follow.txt', 'w') as file:
            file.write('Sorry, the cake is a lie.')
