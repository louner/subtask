import os

class FileReader:
    def __init__(self, fname):
        self.fin = open(fname, 'r')
        self.lineNum = 0

    def readLines(self):
        while 1:
            line = self.fin.readline().rstrip()
            self.lineNum += 1
            if len(line) < 1: break

            self.processLine(line)

        self.finalize()

    def processLine(self, line):
        assert True

    def finalize(self):
        assert True

def mergeSortQuery(filesDir):
    filesNum = 256
    while filesNum > 1:
        queryFiles = os.listdir(filesDir)
        filesNum = len(queryFiles)

        i = 0
        while i+1 < filesNum:
            fileA = open(filesDir+queryFiles[i], 'r')
            fileB = open(filesDir+queryFiles[i+1], 'r')

            fout = open(filesDir+'tmp.'+str(i/2), 'w')

            lineA = fileA.readline()
            lineB = fileB.readline()

            while len(lineA) >= 1 and len(lineB) >= 1:
                if lineA < lineB:
                    fout.write(lineA)
                    lineA = fileA.readline()

                elif lineB < lineA:
                    fout.write(lineB)
                    lineB = fileB.readline()

                else:
                    fout.write(lineA)
                    lineA = fileA.readline()
                    lineB = fileB.readline()

            while len(lineB) >= 1:
                fout.write(lineB)
                lineB = fileB.readline()

            while len(lineA) >= 1:
                fout.write(lineA)
                lineA = fileA.readline()

            fileA.close()
            fileB.close()
            fout.close()

            os.remove(filesDir+queryFiles[i])
            os.remove(filesDir+queryFiles[i+1])

            i += 2
