import os
import time

from Classifier.classifier import cluster_by_classifier, load_model, write_clusters
from eval import val
from meta_blocking import *


def classifier_main():
    from Classifier.eval_classifier import main
    main()


def block_main():
    root = 'data/0-5000#5000-10000'
    # root = 'data/0-1000#1000-2000'
    model_path = os.path.join(root, 'clf.model')
    real_path = os.path.join(root, 'test_real.csv')
    pred_path = os.path.join(root, 'test_pred.csv')
    dataset = Dataset.from_csv(os.path.join(root, 'test.csv'))
    group = MultiBlocking([
        (TokenBlocking(3, 3), 'ssn', 1),
        # (SoundexBlocking(), 'fname', 1),
        # (SoundexBlocking(), 'lname', 1),
        # (SoundexBlocking(), 'stadd', 1),
    ])
    # blocking = SoundexBlocking()
    blocks = group.blocking(dataset, 0.05)
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


if __name__ == '__main__':
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}]')
    # classifier_main()
    block_main()
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}]')
