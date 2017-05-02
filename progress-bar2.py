# -*- coding: utf-8 -*-
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    if iteration == total: 
        print()

from time import sleep

items = list(range(0, 57))
i = 0
l = len(items)


# printProgressBar(i, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
for item in items:
    # Do stuff...
    sleep(0.1)
    # Update Progress Bar
    i += 1
    printProgressBar(i, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
