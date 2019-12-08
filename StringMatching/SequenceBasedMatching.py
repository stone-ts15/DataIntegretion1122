def _edit_distance(sa, sb):
    li, lj = len(sa), len(sb)
    d = [[0 for _ in range(lj + 1)] for _ in range(li + 1)]
    for j in range(1, lj + 1):
        d[0][j] = j
    for i in range(1, li + 1):
        d[i][0] = i
    for i in range(1, li + 1):
        for j in range(1, lj + 1):
            d[i][j] = min(d[i - 1][j - 1] + (0 if sa[i - 1] == sb[j - 1] else 1),
                          d[i - 1][j] + 1,
                          d[i][j - 1] + 1)
    return d[li][lj]


def edit_distance(sa, sb):
    return 1 - _edit_distance(sa, sb) / max(len(sa), len(sb))


def needleman_wunch(sa, sb):
    cg = 1
    cb = 2
    cp = -1

    def cxy(x, y):
        if x == y:
            return cb
        return cp

    li, lj = len(sa), len(sb)
    s = [[0 for _ in range(lj + 1)] for _ in range(li + 1)]
    for j in range(1, lj + 1):
        s[0][j] = -j * cg
    for i in range(1, li + 1):
        s[i][0] = -i * cg
    for i in range(1, li + 1):
        for j in range(1, lj + 1):
            s[i][j] = max(s[i - 1][j - 1] + cxy(sa[i - 1], sb[j - 1]),
                          s[i - 1][j] - cg,
                          s[i][j - 1] - cg)
    ml = max(li, lj)
    ms = ml * cp
    # modify to be different from edit_distance
    return (s[li][lj] - ms) / (min(li, lj) * cb - ms)


def affine_gap(sa, sb):
    c0 = 1.0
    cr = 0.5
    cb = 2
    cp = -1

    def cxy(x, y):
        if x == y:
            return cb
        return cp

    li, lj = len(sa), len(sb)
    s = [[0.0 for _ in range(lj + 1)] for _ in range(li + 1)]
    m = [[0.0 for _ in range(lj + 1)] for _ in range(li + 1)]
    ix = [[0.0 for _ in range(lj + 1)] for _ in range(li + 1)]
    iy = [[0.0 for _ in range(lj + 1)] for _ in range(li + 1)]
    ix[0][0] = iy[0][0] = -c0
    for j in range(1, lj + 1):
        m[0][j] = -j * c0
        ix[0][j] = ix[0][j - 1] - cr
        iy[0][j] = iy[0][j - 1] - cr
    for i in range(1, li + 1):
        m[i][0] = -i * c0
        iy[i][0] = iy[i - 1][0] - cr
        ix[i][0] = ix[i - 1][0] - cr
    for i in range(1, li + 1):
        for j in range(1, lj + 1):
            m[i][j] = max(m[i - 1][j - 1], ix[i - 1][j - 1], iy[i - 1][j - 1]) + cxy(sa[i - 1], sb[j - 1])
            ix[i][j] = max(m[i - 1][j] - c0, ix[i - 1][j] - cr)
            iy[i][j] = max(m[i][j - 1] - c0, iy[i][j - 1] - cr)
            s[i][j] = max(m[i][j], ix[i][j], iy[i][j])
    ml = max(li, lj)
    min_l = min(li, lj)
    ms = min_l * cp - (0 if ml == min_l else (c0 + cr * (ml - min_l - 1)))
    return (s[li][lj] - ms) / (min_l * cb - ms)


def smith_waterman(sa, sb):
    if len(sa) == 0 or len(sb) == 0:
        return 0
    cg = 1
    cb = 2
    cp = -1

    def cxy(x, y):
        if x == y:
            return cb
        return cp

    li, lj = len(sa), len(sb)
    s = [[0 for _ in range(lj + 1)] for _ in range(li + 1)]
    r = 0
    for i in range(1, li + 1):
        for j in range(1, lj + 1):
            s[i][j] = max(0,
                          s[i - 1][j - 1] + cxy(sa[i - 1], sb[j - 1]),
                          s[i - 1][j] - cg,
                          s[i][j - 1] - cg)
            r = max(r, s[i][j])

    return r / (min(li, lj) * cb)


def jaro(sa, sb):
    d = {}
    for i, s in enumerate(sa):
        ds = d.get(s, [])
        ds.append(i)
        d[s] = ds
    c = 0
    ca = []
    cb = ''
    for s in sb:
        ds = d.get(s, [])
        if len(ds) > 0:
            ca.append((ds[0], s))
            ds.pop(0)
            cb += s
            c += 1
    ca.sort(key=lambda x: x[0])
    ca = ''.join(x[1] for x in ca)
    t = _edit_distance(ca, cb)
    # print(f'ca:{ca}, cb: {cb}, c: {c}, t: {t}')
    if c == 0:
        return 0
    return 1 / 3 * (c / len(sa) + c / len(sb) + (c - t / 2) / c)


def jaro_winkler(sa, sb):
    pl = 0
    for a, b in zip(sa, sb):
        if a != b:
            break
        pl += 1
    pw = 1 / max(len(sa), len(sb))
    k = pl * pw
    return (1 - k) * jaro(sa, sb) + k


def equal(sa, sb):
    return 1 if sa == sb else 0


def main():
    sa = 'David Smith'
    sb = 'David Richardson Smith'
    print(edit_distance(sa, sb))
    print(needleman_wunch(sa, sb))
    print(affine_gap(sa, sb))
    print(smith_waterman(sa, sb))
    print(jaro(sa, sb))
    print(jaro_winkler(sa, sb))


if __name__ == '__main__':
    main()
