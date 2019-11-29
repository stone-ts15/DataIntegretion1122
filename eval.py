import csv
from collections import OrderedDict
import data
import meta_blocking
import json


def save_benchmark(end=1000):
    with open('data\\benchmark.csv', 'w', encoding='utf-8', newline='') as fo:
        writer = csv.writer(fo)
        with open('data\\train.csv', 'r', encoding='utf-8') as fi:
            reader = csv.reader(fi)
            for line in reader:
                ruid = int(line[1])
                if ruid < 1000:
                    writer.writerow(line)
                # cuid = int(line[0])
                # if cuid < 1000:
                #     writer.writerow(line)


def calc_real_pairs():
    ans = OrderedDict()
    with open('benchmark.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for line in reader:
            cuid = int(line[0])
            recid = int(line[1])
            if cuid in ans:
                ans[cuid].append(recid)
            else:
                ans[cuid] = [recid]
    
    with open('bm_real.csv', 'w', encoding='utf-8', newline='') as fo:
        writer = csv.writer(fo)
        for cuid, recs in ans.items():
            writer.writerow(recs)


def calc_npairs(lis):
    return sum(a * (a - 1) // 2 for a in lis)


def val(pred_file, real_file):
    pairs_real = []
    cid_map = {}
    with open(real_file, 'r', encoding='utf-8') as fr:
        reader_r = csv.reader(fr)
        for i, line in enumerate(reader_r):
            pairs_real.append(len(line))
            for recid in line:
                cid_map[int(recid)] = i
    all_in_real = calc_npairs(pairs_real)

    num_match_pairs = 0
    pairs_pred = []
    with open(pred_file, 'r', encoding='utf-8') as fp:
        reader_p = csv.reader(fp)
        for line in reader_p:
            pairs_pred.append(len(line))

            matches = {}
            for recid in line:
                cuid = cid_map[int(recid)]
                if cuid in matches:
                    matches[cuid] += 1
                else:
                    matches[cuid] = 1

            num_match_pairs += calc_npairs(matches.values())
    all_in_pred = calc_npairs(pairs_pred)

    precision = num_match_pairs / all_in_pred
    recall = num_match_pairs / all_in_real
    f1 = (2 * precision * recall) / (precision + recall)

    return precision, recall, f1








if __name__ == "__main__":
    # calc_real_pairs()
    # print(val('data\\bm_real.csv', 'data\\bm_fake.csv'))
    save_benchmark(1000)
    

