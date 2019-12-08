from Classifier.classifier import group_cluster_by_classifier, cluster_by_classifier
import time
from multiprocessing import Pool
from functools import partial

PROCESS_NUM = 32


def cluster_by_key(clusters, fk):
    d = {}
    for cluster in clusters:
        k = fk(cluster[0])
        if k is None:
            d[cluster[0].ruid] = cluster
            continue
        if k in d:
            d[k].extend(cluster)
        else:
            d[k] = cluster
    results = list(d.values())
    print(
        f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] [cluster_by_key] {len(clusters)} -> {len(results)}')
    return results


def _process_one(ti_cs, max_refine_size, clf, all_clusters):
    ti, cs = ti_cs
    if ti % 10000 == 0:
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] [cluster_by_blocking] {ti}/{all_clusters}')
    start = 0
    _results = []
    while start < len(cs):
        _results.extend(group_cluster_by_classifier(cs[start:start + max_refine_size], clf))
        start += max_refine_size
    return _results


def cluster_by_blocking(clusters, fk, max_refine_size, clf):
    d = {}
    for cluster in clusters:
        k = fk(cluster[0])
        if k is None:
            d[cluster[0].ruid] = [cluster]
            continue
        if k in d:
            d[k].append(cluster)
        else:
            d[k] = [cluster]
    results = []

    with Pool(PROCESS_NUM) as p:
        rs = p.map(partial(_process_one, max_refine_size=max_refine_size, clf=clf,
                           all_clusters=len(d)), enumerate(d.values()))
    for r in rs:
        results.extend(r)

    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}]'
          f' [cluster_by_blocking] {len(clusters)} -> {len(results)}')

    return results


def _refine_one(ti_cluster, clf, n_clusters):
    ti, cluster = ti_cluster
    if ti % 10000 == 0:
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] [refine_clusters] {ti}/{n_clusters}')
    return cluster_by_classifier(cluster, clf)


def refine_clusters(clusters, clf):
    results = []
    with Pool(PROCESS_NUM) as p:
        rs = p.map(partial(_refine_one, clf=clf,
                           n_clusters=len(clusters)), enumerate(clusters))
    for r in rs:
        results.extend(r)
    print(
        f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] [refine_clusters] {len(clusters)} -> {len(results)}')
    return results
