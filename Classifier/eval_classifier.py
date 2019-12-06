import os
import csv
import random
from data import Dataset
from collections import OrderedDict
from Classifier.similarity import get_similarity
from Classifier.classifier import *
from sklearn.metrics import accuracy_score, recall_score, f1_score
from eval import val

DATA_ROOT = os.path.abspath(os.path.join(__file__, '../../data'))


def _get_dataset_root(train_cuid_range, test_cuid_range):
    root = os.path.join(DATA_ROOT,
                        f'{train_cuid_range[0]}-{train_cuid_range[1]}#{test_cuid_range[0]}-{test_cuid_range[1]}')
    if not os.path.exists(root):
        os.makedirs(root)
    return root


def get_ground_truth(record_filename, label_filename):
    ans = OrderedDict()
    with open(record_filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for line in reader:
            cuid = int(line[0])
            recid = int(line[1])
            if cuid in ans:
                ans[cuid].append(recid)
            else:
                ans[cuid] = [recid]

    with open(label_filename, 'w', encoding='utf-8', newline='') as fo:
        writer = csv.writer(fo)
        for cuid, recs in ans.items():
            writer.writerow(recs)


def prepare_records(train_cuid_range, test_cuid_range):
    root = _get_dataset_root(train_cuid_range, test_cuid_range)
    train_count = test_count = 0
    with open(os.path.join(root, 'train.csv'), 'w', encoding='utf-8', newline='') as train_out:
        with open(os.path.join(root, 'test.csv'), 'w', encoding='utf-8', newline='') as test_out:
            train_writer = csv.writer(train_out)
            test_writer = csv.writer(test_out)
            with open(os.path.join(DATA_ROOT, 'train.csv'), 'r', encoding='utf-8') as fin:
                reader = csv.reader(fin)
                for line in reader:
                    cuid = int(line[0])
                    if train_cuid_range[0] < cuid < train_cuid_range[1]:
                        train_count += 1
                        train_writer.writerow(line)
                    if test_cuid_range[0] < cuid < test_cuid_range[1]:
                        test_count += 1
                        test_writer.writerow(line)
    total_count = train_count + test_count
    get_ground_truth(os.path.join(root, 'train.csv'), os.path.join(root, 'train_real.csv'))
    get_ground_truth(os.path.join(root, 'test.csv'), os.path.join(root, 'test_real.csv'))
    print(f'total: {total_count}')
    print(f'train: {train_count}/{total_count} = {train_count / total_count}')
    print(f'test: {test_count}/{total_count} = {test_count / total_count}')


def prepare_dataset(root, data_rate=1.0, positive_rate=0.5):
    def _prepare(record_filename, data_filename):
        dataset = Dataset(record_filename)
        clusters = list(dataset.rows_oc.values())
        all_positive_count = sum(len(v) * (len(v) - 1) / 2 for v in clusters)
        all_count = int(all_positive_count / positive_rate * data_rate)
        n_entities = len(clusters)
        n_positive = n_negative = 0

        with open(data_filename, 'w') as data_out:
            for i in range(all_count):
                if i % 100 == 0:
                    print(f'[{data_filename}] Process: {i}')
                if random.random() < positive_rate:
                    label = 1
                    while True:
                        e = random.randint(0, n_entities - 1)
                        if len(clusters[e]) > 1:
                            break
                    items = random.sample(clusters[e], 2)
                    assert items[0].cuid == items[1].cuid
                    n_positive += 1
                else:
                    label = 0
                    items = [x[random.randint(0, len(x) - 1)] for x in random.sample(clusters, 2)]
                    assert items[0].cuid != items[1].cuid
                    n_negative += 1
                feature = get_similarity(*items)
                data_out.write(f'{items[0].ruid} {items[1].ruid} {label} {" ".join(map(str, feature))}\n')
        print(f'[{data_filename}] positive: {n_positive}, negative: {n_negative}, total: {n_positive + n_negative}')

    _prepare(os.path.join(root, 'train.csv'), os.path.join(root, 'train.txt'))
    _prepare(os.path.join(root, 'test.csv'), os.path.join(root, 'test.txt'))


def load_dataset(root):
    def _load(filename):
        features = []
        labels = []
        with open(filename) as fin:
            for line in fin:
                items = line.strip().split()
                features.append([float(x) for x in items[3:]])
                labels.append(int(items[2]))
        return features, labels

    return _load(os.path.join(root, 'train.txt')) + _load(os.path.join(root, 'test.txt'))


def evaluate_classifier(real_labels, pred_labels):
    print('acc:', accuracy_score(real_labels, pred_labels))
    print('recall:', recall_score(real_labels, pred_labels))
    print('f1:', f1_score(real_labels, pred_labels))


def evaluate_cluster(data_path, model_path, real_path, pred_path):
    dataset = Dataset(data_path)
    clf = load_model(model_path)
    clusters = cluster_by_classifier(dataset, clf)
    for cluster in clusters:
        cluster.sort()
    with open(pred_path, 'w') as out:
        for cluster in clusters:
            out.write(','.join(map(str, cluster)) + '\n')
    precision, recall, f1 = val(pred_path, real_path)
    print(f'precision: {precision}')
    print(f'recall: {recall}')
    print(f'f1: {f1}')


def main():
    random.seed(1)
    train_cuid_range = (0, 1000)
    test_cuid_range = (1000, 2000)
    root = _get_dataset_root(train_cuid_range, test_cuid_range)
    prepare_records(train_cuid_range, test_cuid_range)
    prepare_dataset(root, positive_rate=0.1)
    train_features, train_labels, test_features, test_labels = load_dataset(root)

    model_path = os.path.join(root, 'DTClassifier.model')
    # clf = load_model(model_path)
    clf = DTClassifier(max_depth=3, random_state=1)
    clf.fit(train_features, train_labels)

    print('========== Train =============')
    pred_labels = clf.predict(train_features)
    print(len(list(filter(lambda x: x == 0, pred_labels))),
          len(list(filter(lambda x: x == 1, pred_labels))))
    evaluate_classifier(train_labels, pred_labels)
    print('==============================')

    print('========== Test =============')
    pred_labels = clf.predict(test_features)
    print(len(list(filter(lambda x: x == 0, pred_labels))),
          len(list(filter(lambda x: x == 1, pred_labels))))
    evaluate_classifier(test_labels, pred_labels)
    print('=============================')

    save_model(clf, model_path)
    tag = 'test'
    evaluate_cluster(os.path.join(root, f'{tag}.csv'),
                     model_path,
                     os.path.join(root, f'{tag}_real.csv'),
                     os.path.join(root, f'{tag}_pred.csv'))


if __name__ == '__main__':
    main()
