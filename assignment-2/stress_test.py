import os
import sys

try:
    iter=int(sys.argv[1])
except:
    iter=50

for i in range(iter):
    os.system("python3 test_generator.py")
    os.system("python3 ref.py random_input.txt > ref.out")
    os.system("python3 assignment-2-19CS30014.py random_input.txt > my.out")
    os.system("diff -s ref.out my.out")

os.system("rm random_input.txt ref.out my.out")