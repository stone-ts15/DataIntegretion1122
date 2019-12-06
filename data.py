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

        with open(csvpath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for i, line in enumerate(reader):
                cuid = int(line[0])
                ruid = int(line[1])
                row = Row(cuid, ruid, tuple(line[2:]))
                self.rows.append(row)
                if cuid in self.rows_oc:
                    self.rows_oc[cuid] = self.rows_oc[cuid] + (row,)
                else:
                    self.rows_oc[cuid] = (row,)

                if i % 20000 == 0:
                    print('read ', i)
