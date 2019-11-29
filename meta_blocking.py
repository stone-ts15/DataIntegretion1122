
from data import Dataset, Row, DisjointSet
import csv


class Method:
    def __init__(self):
        self.blocks = {'null': tuple()}
        self.nrows = 0

    def __call__(self, ds: Dataset, key: str):
        self.nrows = ds.nrows

    def block_to_result(self, threshold: int):
        pair_map = {}
        for k, tp in self.blocks.items():
            for i in tp:
                for j in tp:
                    if i < j:
                        pair = (i, j)
                        if pair in pair_map:
                            pair_map[pair] += 1
                        else:
                            pair_map[pair] = 1

        matches = []
        for pair, nedges in pair_map.items():
            if nedges >= threshold:
                matches.append(pair)


        print(matches[:20])

        result = DisjointSet(self.nrows)
        result.update(matches)
        return result.clusters()


class TokenBlocking(Method):
    def __init__(self, attr_len, tk_len, interval):
        super(TokenBlocking, self).__init__()
        self.attr_len = attr_len
        self.tk_len = tk_len
        self.interval = interval

    def __call__(self, ds: Dataset, key):
        for i, row in enumerate(ds.rows):
            attr = row[key]
            if attr == '000000000':
                self.blocks['null'] = self.blocks['null'] + (row.ruid, )
                continue

            start = 0
            while True:
                end = start + self.tk_len

                if end > len(attr):
                    break

                token = attr[start:end]
                if token in self.blocks:
                    self.blocks[token] = self.blocks[token] + (row.ruid, )
                else:
                    self.blocks[token] = (row.ruid, )

                start += self.interval

            if i % 20000 == 0:
                print('match ', i)

        dep_keys = ['null']
        for k, v in self.blocks.items():
            if len(v) <= 1:
                dep_keys.append(k)

        for k in dep_keys:
            del self.blocks[k]
        return self.blocks


class SoundexBlocking(Method):
    def __init__(self):
        super(SoundexBlocking, self).__init__()
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
        for i, row in enumerate(ds.rows):
            attr = row[key]
            # attr: Kezun
            # attr: Kezhun
            encoding = self.soundex(attr)
            if encoding is None:
                self.blocks['null'] = self.blocks['null'] + (row.ruid, )
                continue

            if encoding in self.blocks:
                self.blocks[encoding] = self.blocks[encoding] + (row.ruid, )
            else:
                self.blocks[encoding] = (row.ruid, )

            if i % 20000 == 0:
                print('match ', i)

        dep_keys = []
        for k, v in self.blocks.items():
            if len(v) <= 1 or dep_keys == 'null':
                dep_keys.append(k)

        for k in dep_keys:
            del self.blocks[k]
        return self.blocks



