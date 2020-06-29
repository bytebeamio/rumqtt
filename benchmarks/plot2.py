import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import collections

# reference: https://www.shanelynn.ie/bar-plots-in-python-using-pandas-dataframes/#:~:text=To%20create%20this%20chart%2C%20place,plot%20command.&text=It's%20simple%20to%20create%20bar,plot()%20command.

def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        width = rect.get_width()
        height = rect.get_height()
        x = rect.get_x()
        y = rect.get_y()
        if width == 0:
            continue
        ax.text(width + 100, y + height/2., width, ha='center', va='center')


# { 'l1': [{'rumqttsync', 10000}, ['rumqttasync': 2000]], 
#   'l2': [{'paho-rust', 10000}] 
# }
def group(labels):
    grouped = collections.OrderedDict()
    for v in labels.values():
        for target, throughput in v.items():
            grouped[target] = []

    for key, value in grouped.items():
        for targets in labels.values():
            if key in targets:
                value.append(targets[key])
            else:
                value.append(np.nan)

    labels = list(labels.keys())
    return (labels, grouped)



def collect(f):
    '''
    Takes the file and returns more meaningful collection for plotting
    :param f: file name
    :return: Dictonary of Id (for legend), Throughput (for values) along with other metadata for y axis marker
    '''
    labels = collections.OrderedDict()
    with open(f) as f:
        for line in f.readlines():
            res = dict(item.split("=") for item in line.split(", "))
            res = {key.strip(): val.strip() for key, val in res.items()}
            # group throughputs by IDs (tokio, smol) etc
            target = res.pop('Id')
            throughput = res.pop('Throughput (messages/sec)')
            parameters = [str(k) + ' = ' + str(v) for k, v in res.items()]
            label = '\n'.join(parameters)
            if label not in labels:
                labels[label] = collections.OrderedDict()
            labels[label][target] = int(throughput)


    return labels 

labels = collect(sys.argv[1])
l, g = group(labels)
plotdata = pd.DataFrame(g, index=l)
ax = plotdata.plot(kind="barh")
autolabel(ax.patches)
plt.tight_layout()
plt.savefig(sys.argv[1] + '.png')
plt.show()
 
