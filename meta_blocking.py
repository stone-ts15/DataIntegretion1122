from data import Dataset, Row, DisjointSet
import csv
import queue


class BlockingMethod:
    debug = {
        'log_interval': 20000
    }

    def __init__(self, edge_weight):
        self.blocks = {'null': []}
        self.nrows = 0
        self.relations = None
        self.block_id_map = None
        self.edge_weight = edge_weight

    def __call__(self, ds: Dataset, key: str):
        self.nrows = ds.nrows

    def update_ds(self, row, attr: str):
        if attr in self.blocks:
            for neighbor_row in self.blocks[attr]:
                if neighbor_row.ruid in row.neighbors:
                    row.neighbors[neighbor_row.ruid][1] += self.edge_weight
                    neighbor_row.neighbors[row.ruid][1] += self.edge_weight
                else:
                    row.neighbors[neighbor_row.ruid] = [neighbor_row, self.edge_weight]
                    neighbor_row.neighbors[row.ruid] = [row, self.edge_weight]

            self.blocks[attr].append(row)
        else:
            self.blocks[attr] = [row]

    def del_outliers(self):
        outlier_keys = []
        for k, v in self.blocks.items():
            if len(v) <= 1 or outlier_keys == 'null':
                outlier_keys.append(k)

        for k in outlier_keys:
            del self.blocks[k]

    def weight_Jaccard(self, threshold: int):
        # Jaccard Weight
        self.relations = [0] * self.nrows

        self.block_id_map = {}
        pair_map = {}
        for i, (block_identifier, ruids) in enumerate(self.blocks.items()):
            self.block_id_map[block_identifier] = i
            for ruid in ruids:
                if self.relations[ruid] == 0:
                    self.relations[ruid] = [i]
                else:
                    self.relations[ruid].append(i)

                ri = ruid
                for rj in ruids:
                    if ri < rj:
                        pair = (ri, rj)
                        if pair in pair_map:
                            pair_map[pair] += 1
                        else:
                            pair_map[pair] = 1
        # it promises that, block ids for each row is in ascending order

        salient_pair_map = []
        for pair, ncommons in pair_map.items():
            ri, rj = pair
            Bi, Bj, Bij = len(self.relations[ri]), len(self.relations[rj]), ncommons
            Jaccard = Bij / (Bi + Bj - Bij)
            if Jaccard >= threshold:
                salient_pair_map.append((ri, rj, Jaccard))

        return salient_pair_map

    def block_to_result(self, threshold: int):
        edge_weight = 1

        pair_map = {}
        for k, tp in self.blocks.items():
            for i in tp:
                for j in tp:
                    if i < j:
                        pair = (i, j)
                        if pair in pair_map:
                            pair_map[pair] += edge_weight
                        else:
                            pair_map[pair] = edge_weight

        matches = []
        for pair, nedges in pair_map.items():
            if nedges >= threshold:
                matches.append((pair, nedges))

        return matches

        # result = DisjointSet(self.nrows)
        # result.update(matches)
        # return result.clusters()


class MultiBlocking:
    def __init__(self, methods: [(BlockingMethod, str)]):
        self.method_details = methods

    def __call__(self, ds: Dataset, threshold_all):
        all_matches = []
        for method, key in self.method_details:
            method(ds, key)
            # all_matches.append(method.block_to_result(threshold_blocking))

        # bfs
        blocks = []
        for row in ds.rows:
            row.visited = False

        for row in ds.rows:
            if row.visited:
                continue

            row.visited = True
            new_block = []
            candidate_queue = queue.Queue(maxsize=2000000)
            candidate_queue.put(row)

            while not candidate_queue.empty():
                cand = candidate_queue.get()
                new_block.append(cand)
                for neighbor_id, (neighbor, weight) in cand.neighbors.items():
                    if not neighbor.visited and weight >= threshold_all:
                        neighbor.visited = True
                        candidate_queue.put(neighbor)

            blocks.append(new_block)

        return blocks



        # shared_blockings = {}
        # match_map = {}
        # for match in all_matches:
        #     for (ri, rj), common_weight in match:
        #         shared_blockings[ri] = shared_blockings.get(ri, 0) + 1
        #         shared_blockings[rj] = shared_blockings.get(rj, 0) + 1
        #         if (ri, rj) not in match_map:
        #             match_map[(ri, rj)] = common_weight
        #         else:
        #             match_map[(ri, rj)] += common_weight
        #
        # matches = []
        # for (ri, rj), weight in match_map.items():
        #     Bi, Bj, Bij = shared_blockings[ri], shared_blockings[rj], weight
        #     Jaccard = Bij / (Bi + Bj - Bij)
        #     if Jaccard >= threshold_all:
        #         matches.append((ri, rj))

        # return matches

    def blocking(self, ds: Dataset, threshold_all):
        matches = self(ds, threshold_all)
        result = DisjointSet(ds)
        result.update(matches)

        ds_blocks = [Dataset.from_rows(ds, cluster) for cluster in result.clusters()]
        return ds_blocks


class FullBlocking(BlockingMethod):
    def __init__(self, edge_weight):
        super(FullBlocking, self).__init__(edge_weight)

    def __call__(self, ds: Dataset, key):
        for i, row in enumerate(ds.rows):
            attr = row[key]
            ruid = row.ruid
            if not attr:
                pass
                # self.blocks['null'].append(row.ruid)

            self.update_ds(row, attr)

            if i % self.debug['log_interval'] == 0:
                print('match ', i)

        # self.del_outliers()
        # return self.blocks


class TokenBlocking(BlockingMethod):
    def __init__(self, tk_len, interval, edge_weight):
        super(TokenBlocking, self).__init__(edge_weight)
        self.tk_len = tk_len
        self.interval = interval

    def __call__(self, ds: Dataset, key):
        for i, row in enumerate(ds.rows):
            attr = row[key]
            ruid = row.ruid
            if attr == '000000000':
                self.blocks['null'].append(ruid)
                continue

            start = 0
            while True:
                end = start + self.tk_len

                if end > len(attr):
                    break

                token = attr[start:end]
                self.update_ds(row, token)

                start += self.interval
            if i % self.debug['log_interval'] == 0:
                print('match ', i)
        self.del_outliers()
        return self.blocks


class SoundexBlocking(BlockingMethod):
    def __init__(self, edge_weight):
        super(SoundexBlocking, self).__init__(edge_weight)
        self.alphabet = {
            1: 'BFPV',
            2: 'CGJKQSXZ',
            3: 'DT',
            4: 'L',
            5: 'MN',
            6: 'R'
        }
        self.abmap = {}
        for digit, letters in self.alphabet.items():
            for l in letters:
                self.abmap[l] = digit

    def soundex(self, s: str):
        if not s:
            return None
        s = s.upper()
        first = s[0]
        last_digit = -1
        result = first
        for ch in s[1:]:
            if ch in 'WH':
                continue
            if ch in self.abmap:
                digit = self.abmap[ch]
                if digit != last_digit:
                    result += str(digit)
                    last_digit = digit
            else:
                last_digit = -1

        result += '0000'
        return result[:4]

    def __call__(self, ds: Dataset, key):
        super(SoundexBlocking, self).__call__(ds, key)
        if ',' not in key:
            for i, row in enumerate(ds.rows):

                attr = row[key]
                ruid = row.ruid
                # attr: Kezun
                # attr: Kezhun
                encoding = self.soundex(attr)
                if encoding is None:
                    self.blocks['null'].append(ruid)
                    continue

                self.update_ds(row, encoding)
        else:
            f, l = key.split(',')
            for i, row in enumerate(ds.rows):
                fname = row['fname']
                lname = row['lname']
                # attr = row[key]
                ruid = row.ruid
                # attr: Kezun
                # attr: Kezhun
                # encoding = self.soundex(attr)
                ef = self.soundex(fname)
                el = self.soundex(lname)
                self.update_ds(row, ef)
                self.update_ds(row, el)
            # if encoding is None:
            #     self.blocks['null'].append(ruid)
            #     continue
            #
            # self.update_ds(row, encoding)

                if i % self.debug['log_interval'] == 0:
                    print('match ', i)

        self.del_outliers()
        return self.blocks
