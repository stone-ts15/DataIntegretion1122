import pickle
import time
import random

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.cluster import k_means_

from .similarity import get_similarity


class RBClassifier:

    def __init__(self):
        pass

    def fit(self, features, labels):
        return

    @staticmethod
    def predict(features):
        labels = []
        for feature in features:
            sim = feature[0] + sum(x / (len(feature) - 1) for x in feature[1:])
            labels.append(1 if sim > 1.0 else 0)
        return labels


DTClassifier = DecisionTreeClassifier
LRClassifier = LogisticRegression
RFClassifier = RandomForestClassifier


def save_model(clf, model_path):
    print(f'Save Model to "{model_path}"')
    with open(model_path, 'wb') as out:
        pickle.dump(clf, out)


def load_model(model_path):
    print(f'Load Model from "{model_path}"')
    with open(model_path, 'rb') as fin:
        clf = pickle.load(fin)
    return clf


def cluster_by_classifier_n2(rows, clf):
    clusters = []
    for i, row in enumerate(rows):
        if i % 100 == 0:
            print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Cluster: {i}/{len(rows)}')
        assign = False
        for cluster in clusters:
            feature = get_similarity(row, cluster[0])
            if clf.predict([feature])[0] == 1:
                cluster.append(row)
                assign = True
                break
        if not assign:
            clusters.append([row])
    clusters = [[r.ruid for r in cluster] for cluster in clusters]
    return clusters


def cluster_by_classifier_n2_2(rows, clf):
    clusters = []
    count = 0
    while len(rows) > 0:
        if count % 100 == 0:
            print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] count: {count}, rest: {len(rows)}')
        count += 1
        row = rows[-1]
        rows.pop()
        _rows = []
        cluster = [row]
        if len(rows) > 0:
            features = tuple(get_similarity(x, row) for x in rows)
            labels = clf.predict(features)
            for r, label in zip(rows, labels):
                if label == 1:
                    cluster.append(r)
                else:
                    _rows.append(r)
        clusters.append(cluster)
        rows = _rows
    clusters = [[r.ruid for r in cluster] for cluster in clusters]
    return clusters


# def _cluster_direct(cs, clf):
#     _cs = []
#     for c in cs:
#         assign = False
#         for _c in _cs:
#             feature = get_similarity(c[0], _c[0])
#             if clf.predict([feature])[0] == 1:
#                 _c.extend(c)
#                 assign = True
#                 break
#         if not assign:
#             _cs.append(c)
#     return _cs


def cluster_by_classifier_fast(rows, clf):
    def _distance(row_a, row_b):
        feature = get_similarity(row_a, row_b)
        return 1 - clf.predict([feature])[0]

    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}]')
    clusters = [row.data for row in rows]
    n_clusters = len(rows) / 3
    k_means_.euclidean_distances = _distance
    k_means = k_means_.KMeans(n_clusters=n_clusters, n_jobs=8, random_state=1)
    print(k_means.fit(clusters))
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}]')
    return []


cluster_by_classifier = cluster_by_classifier_n2_2
