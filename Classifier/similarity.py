from StringMatching import *

# 'cuid', 'ruid', 'ssn', 'fname', 'minit', 'lname', 'stnum', 'stadd', 'apmt', 'city', 'state', 'zip'


def get_similarity(row_a, row_b):
    # ssn
    ssn_feature = edit_distance(row_a['ssn'], row_b['ssn'])

    # fname + minit + lname
    name_feature = affine_gap(
        ' '.join(row_a[x] for x in ('fname', 'minit', 'lname')),
        ' '.join(row_b[x] for x in ('fname', 'minit', 'lname'))
    )

    # stnum
    stnum_feature = jaro(row_a['stnum'], row_b['stnum'])

    # stadd
    stadd_feature = jaro_winkler(row_a['stadd'], row_b['stadd'])

    # apmt
    apmt_feature = edit_distance(row_a['apmt'], row_b['apmt'])

    # city
    city_feature = smith_waterman(row_a['city'], row_b['city'])

    # state
    state_feature = equal(row_a['state'], row_b['state'])

    # zip
    zip_feature = needleman_wunch(row_a['zip'], row_b['zip'])

    features = (
        ssn_feature,
        name_feature,
        stnum_feature,
        stadd_feature,
        apmt_feature,
        city_feature,
        state_feature,
        zip_feature
    )

    return features
