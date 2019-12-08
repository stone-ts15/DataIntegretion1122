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
        self.relations = []
        self.neighbors = {}

        # neighbors: {id: weight}
        self.visited = False

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        elif isinstance(key, str):
            return self.data[self.idx[key]]


class Vertex:
    def __init__(self, row: Row):
        self.row_ref = row
        self.ruid = row.ruid
        self.cuid = row.cuid
        self.relations = ()
        self.neighbors = []


class Dataset:
    def __init__(self):
        self.rows = []
        self.rows_oc = {}
        self.nrows = 0
        self.vertices = []

    @classmethod
    def from_csv(cls, csvpath):
        ds = Dataset()
        with open(csvpath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for i, line in enumerate(reader):
                cuid = int(line[0])
                ruid = int(line[1])
                row = Row(cuid, ruid, tuple(line[2:]))

                # ruid = int(line[0])
                # row = Row(-1, ruid, tuple(line[1:]))

                ds.rows.append(row)

                # vertex = Vertex(ruid, cuid)
                # ds.vertices.append(vertex)

                if cuid in ds.rows_oc:
                    ds.rows_oc[cuid].append(row)
                else:
                    ds.rows_oc[cuid] = [row]

                if i % 200000 == 0:
                    print('read ', i)
        ds.nrows = len(ds.rows)
        return ds

    @classmethod
    def from_rows(cls, src, cluster: [int]):
        ds = Dataset()
        ds.rows = [src.rows[ruid] for ruid in cluster]
        ds.nrows = len(cluster)
        return ds


class DisjointSet:
    def __init__(self, ds):
        # print(nrows)
        self.nrows = ds.nrows
        self.root = {row.ruid: -1 for row in ds.rows}
        self.cid_map = None

    def find_root(self, i: int):
        root_i = self.root.get(i, -1)
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

            if roota == rootb:
                continue

            depth_a, depth_b = -self.root.get(roota, -1), -self.root.get(rootb, -1)
            if depth_a >= depth_b:
                self.root[rootb] = roota
                self.root[roota] = self.root.get(roota, -1) - depth_b
            else:
                self.root[roota] = rootb
                self.root[rootb] = self.root.get(rootb, -1) - depth_a

    def clusters(self):
        cid_map = {}
        for node in self.root:
            root = self.find_root(node)
            if root not in cid_map:
                cid_map[root] = [node]
            else:
                cid_map[root].append(node)
        self.cid_map = cid_map
        return cid_map.values()
