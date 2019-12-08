import os
import time

from Classifier.classifier import cluster_by_classifier, load_model, write_clusters
from eval import val
from meta_blocking import *
from cluster import *


def classifier_main():
    from Classifier.eval_classifier import main
    main()


def block_main():
    # root = 'data/0-1000#1000-2000'
    root = 'data/0-5000#5000-10000'
    # root = 'data/0-1200894#1200894-2401789'
    model_path = os.path.join(root, 'clf.model')
    real_path = os.path.join(root, 'test_real.csv')
    pred_path = os.path.join(root, 'test_pred.csv')
    dataset = Dataset.from_csv(os.path.join(root, 'test.csv'))

    group = MultiBlocking([
        (FullBlocking(1), 'ssn'),
        (SoundexBlocking(1), 'fname'),
        (SoundexBlocking(1), 'lname'),
        (SoundexBlocking(1), 'stadd'),
        (SoundexBlocking(1), 'city'),
        (FullBlocking(1), 'zip'),
    ])
    # blocking = SoundexBlocking()
    blocks = group(dataset, 3)
    # rows_dict = {}
    # for row in dataset.rows:
    #     rows_dict[row.ruid] = row
    clusters = []
    clf = load_model(model_path)
    # n_blocks = len(blocks)
    for i, block in enumerate(blocks):
        # if i % 1000 == 0:
        #     print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] {i}/{n_blocks}')
        clusters.extend(cluster_by_classifier(block, clf))
    write_clusters(clusters, pred_path)
    precision, recall, f1 = val(pred_path, real_path)
    print(f'precision: {precision}')
    print(f'recall: {recall}')
    print(f'f1: {f1}')


def cluster_main():
    # root = 'data/0-1000#1000-2000'
    # root = 'data/0-5000#5000-10000'
    root = 'data/0-1200894#1200894-2401789'
    model_path = os.path.join(root, 'clf.model')
    real_path = os.path.join(root, 'test_real.csv')
    pred_path = os.path.join(root, 'test_pred.csv')
    dataset = Dataset.from_csv(os.path.join(root, 'test.csv'))

    # model_path = os.path.join('data/0-1200894#1200894-2401789', 'clf.model')
    # real_path = os.path.join('data', 'train_real.csv')
    # pred_path = os.path.join('data', 'train_pred_2.csv')
    # dataset = Dataset.from_csv(os.path.join('data', 'train.csv'))

    clf = load_model(model_path)
    sound = SoundexBlocking(1)

    def soundex(x):
        return sound.soundex(x) or ' '

    def ssn_key(x, ti):
        if x[0] == '000000000':
            return None
        return x[0][:ti] + x[0][ti + 1:]

    clusters = [[row] for row in dataset.rows]
    clusters = cluster_by_key(clusters, lambda x: x[0] if x[0] != '000000000' else None)
    # for i in range(9):
    #     clusters = cluster_by_blocking(clusters, partial(ssn_key, ti=i), 201, clf)

    clusters = cluster_by_key(clusters, lambda x: x[1] + x[3])
    clusters = cluster_by_key(clusters, lambda x: x[4] + x[6])
    clusters = refine_clusters(clusters, clf)
    # clusters = cluster_by_blocking(clusters, lambda x: x[4] + x[6], 201, clf)
    clusters = cluster_by_blocking(clusters, lambda x: soundex(x[5]) + soundex(x[7]), 201, clf)
    clusters = cluster_by_blocking(clusters, lambda x: soundex(x[1]) + soundex(x[3]), 201, clf)
    # clusters = cluster_by_blocking(clusters, lambda x: soundex(x[1]) + soundex(x[7]), 201, clf)
    # clusters = cluster_by_blocking(clusters, lambda x: soundex(x[3]) + x[9], 201, clf)

    clusters = [[r.ruid for r in cluster] for cluster in clusters]
    write_clusters(clusters, pred_path)

    precision, recall, f1 = val(pred_path, real_path)
    print(f'precision: {precision}')
    print(f'recall: {recall}')
    print(f'f1: {f1}')


if __name__ == '__main__':
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'[{start_time}]')
    # classifier_main()
    cluster_main()
    print(f'[{start_time}] -> [{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}]')
