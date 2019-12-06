from data import Dataset, Row


class Method:
    pass


class TokenBlocking(Method):
    def __init__(self, attr_len, tk_len, interval):
        self.attr_len = attr_len
        self.tk_len = tk_len
        self.interval = interval
        self.blocks = {'null': tuple()}

    def __call__(self, ds: Dataset, key):
        for i, row in enumerate(ds.rows):
            attr = row[key]
            if attr == '000000000':
                self.blocks['null'] = self.blocks['null'] + (row.ruid,)
                continue

            start = 0
            while True:
                end = start + self.tk_len

                if end > len(attr):
                    break

                token = attr[start:end]
                if token in self.blocks:
                    self.blocks[token] = self.blocks[token] + (row.ruid,)
                else:
                    self.blocks[token] = (row.ruid,)

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
            return '0000'

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
