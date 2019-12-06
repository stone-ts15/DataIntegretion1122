import pickle

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier

from .similarity import get_similarity


class ClassifierBase:

    def __init__(self):
        pass

    def fit(self, features, labels):
        pass

    def predict(self, features):
        pass


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


def cluster_by_classifier(dataset, clf):
    clusters = []
    rows = dataset.rows
    for i, row in enumerate(rows):
        if i % 100 == 0:
            print(f'Cluster: {i}/{len(rows)}')
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
