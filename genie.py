import random
import os
from processData import *
from parameter import *
import time

def testMergeSort():
    for i in range(16):
        a = random.random()
        open('./tmp/'+str(i), 'w').write(str(a)+'\n'+str(a+1)+'\n')

    mergeSortQuery('./tmp/')
