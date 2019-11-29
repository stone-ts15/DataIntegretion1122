import csv


class Row:
    title = (
        'ssn',
        'fname',
        'minit',
        'lname',
        'stnum',
        'stadd',
        'apmt',
        'city',
        'state',
        'zip'
    )
    idx = {k: i for i, k in enumerate(title)}

    def __init__(self, cuid, ruid, data: tuple):
        self.cuid = cuid
        self.ruid = ruid
        self.data = data

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        elif isinstance(key, str):
            return self.data[self.idx[key]]


class Dataset:
    def __init__(self, csvpath):
        self.rows = []
        self.rows_oc = {}
        self.nrows = 0

        with open(csvpath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for i, line in enumerate(reader):
                cuid = int(line[0])
                ruid = int(line[1])
                row = Row(cuid, ruid, tuple(line[2:]))
                self.rows.append(row)
                if cuid in self.rows_oc:
                    self.rows_oc[cuid] = self.rows_oc[cuid] + (row, )
                else:
                    self.rows_oc[cuid] = (row, )

                if i % 20000 == 0:
                    print('read ', i)
            self.nrows = len(self.rows)


class DisjointSet:
    def __init__(self, nrows: int):
        print(nrows)
        self.nrows = nrows
        self.root = [-1] * nrows
        self.cid_map = None

    def find_root(self, i: int):
        root_i = self.root[i]
        if root_i >= 0:
            root_2 = self.find_root(root_i)
            self.root[i] = root_2
            return root_2
        else:
            return i

    def update(self, pairs: [tuple]):
        for pair in pairs:
            a, b = pair
            roota = self.find_root(a)
            rootb = self.find_root(b)

            depth_a, depth_b = -self.root[roota], -self.root[rootb]
            if depth_a >= depth_b:
                self.root[rootb] = roota
                self.root[roota] -= depth_b
            else:
                self.root[roota] = rootb
                self.root[rootb] -= depth_a

    def clusters(self):
        cid_map = {}
        for i, node in enumerate(self.root):
            root = self.find_root(node)
            if root not in cid_map:
                cid_map[root] = [i]
            else:
                cid_map[root].append(i)
        self.cid_map = cid_map
        return cid_map

