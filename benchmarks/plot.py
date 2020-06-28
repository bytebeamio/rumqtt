import collections

import matplotlib.pyplot as plt
import numpy as np
import sys
from typing import List, Tuple, Dict


class Benchmark:
    labels: List[str]
    count = 0
    spacing = 0.1
    height = 0.025
    figure = None
    ax = None
    y = None

    def __init__(self, labels):
        count = len(labels)
        self.labels = labels
        start, stop = 0, count * self.spacing
        self.y = np.linspace(start=start, stop=stop, num=count)
        print(self.y)
        self.fig, self.ax = plt.subplots()

        # Add some text for labels, title and custom x-axis tick labels, etc.
        self.ax.set_xlabel('Throughput (messages/sec)')
        self.ax.set_yticks(self.y)
        self.ax.set_yticklabels(labels, va='center')
        self.fig.tight_layout()

    @staticmethod
    def autolabel(ax, rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect in rects:
            width = rect.get_width()
            x = rect.get_x()
            ax.text(width + 2, rect.get_y() + rect.get_height() / 2.,
                    '%.2f' % width,
                    ha='left', va='center', color='brown')

    def plot(self, target: str, values: List[int]):
        print(self.spacing)
        rects = self.ax.barh(self.y + (self.count * self.height), values, height=self.height, label=target)
        self.ax.legend()
        self.autolabel(self.ax, rects)
        self.count += 1

    @staticmethod
    def save(filename):
        plt.savefig(filename)

    @staticmethod
    def show():
        plt.show()


def collect(f) -> Tuple[Dict, Dict]:
    '''
    Takes the file and returns more meaningful collection for plotting
    :param f: file name
    :return: Dictonary of Id (for legend), Throughput (for values) along with other metadata for y axis marker
    '''
    # list of values for a given target
    out = collections.defaultdict(list)
    # ordered dict to ignore duplicate labels. Will be converted to the list with just keys at the end
    labels = collections.OrderedDict()
    with open(f) as f:
        for line in f.readlines():
            res = dict(item.split("=") for item in line.split(", "))
            res = {key.strip(): val.strip() for key, val in res.items()}
            # group throughputs by IDs (tokio, smol) etc
            target = res.pop('Id')
            throughput = res.pop('Throughput (messages/sec)')
            out[target].append(int(throughput))
            parameters = [str(k) + ' = ' + str(v) for k, v in res.items()]
            if '\n'.join(parameters) not in labels:
                labels['\n'.join(parameters)] = collections.OrderedDict()
            labels['\n'.join(parameters)][target] = int(throughput)

    return labels, out

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
                value.append(np.NaN)

    labels = list(labels.keys())
    print(labels)
    print(grouped)
    return (labels, grouped)

labels, out = collect(sys.argv[1])
l, out = group(labels)
benchmark = Benchmark(l)

 
for target, throughputs in out.items():
    benchmark.plot(target, throughputs)
benchmark.save(sys.argv[1] + '.png')
benchmark.show()
