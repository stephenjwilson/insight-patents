import sys


def main(nodes, edges):
    """ Filters the edges list by nodes that actually exist"""
    fl = open(nodes).readlines()

    nodes = {}
    for line in fl:
        if line != '\n':
            line = line.strip().split(',')
            if line[0].isdigit():
                nodes[line[0]] = 1

    to_keep = open('trim_edges.csv', 'w')
    f = open(edges)
    for line in f:
        line = line.strip().split(',')

        try:
            nodes[line[0]]
        except:
            continue

        try:
            nodes[line[1]]
        except:
            continue
        to_keep.write(','.join(line) + '\n')
    to_keep.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
