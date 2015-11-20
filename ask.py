from pattern import findSubtaskByPatternsFromQuery
import sys

task = ' '.join(sys.argv[1:])
subtasks = findSubtaskByPatternsFromQuery(task)
for subtask in subtasks:
    print subtask
