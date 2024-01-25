import matplotlib
import matplotlib.pyplot as plt
import csv

matplotlib.use('TkAgg')


def plot_types(x, y):
    plt.figure(111)
    # fig, ax = plt.subplots()
    barlist = plt.bar(x, y)
    plt.grid(axis='y', alpha=0.75)
    plt.xlabel('Number of reactions')
    plt.ylabel('Number of pathways')
    plt.title("Number of pathways per number of reactions")
    plt.show()


try:
    file_csv = "./data/reactome/distribution-reactions-pathways.csv"

    map = {}
    with open(file_csv, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        # for i, row in enumerate(reader):
        for row in reader:
            # map[i] = int(row[1])
            map[int(row[0])] = int(row[1])
    x = map.keys()
    y = map.values()
    print(x)
    print(y)
    plot_types(x, y)

except Exception as e:
    print(e)


