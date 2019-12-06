from multiset import Multiset


def overlap_jaccard(sa, sb, n=3):
    def _cal_n_grams(s, n):
        t = '#' * (n - 1)
        s = t + s + t
        return [s[i:i + n] for i in range(len(s) - n + 1)]
    ngs_a = Multiset(_cal_n_grams(sa, n))
    ngs_b = Multiset(_cal_n_grams(sb, n))
    return len(ngs_a & ngs_b)/ len(ngs_a | ngs_b)


def main():
    sa = 'dave'
    sb = 'dav'
    print(overlap_jaccard(sa, sb, 2))


if __name__ == '__main__':
    main()
