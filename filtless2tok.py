import fileinput
for line in fileinput.input():
    if len(line.split()) >= 3:
        print line[:-1]
