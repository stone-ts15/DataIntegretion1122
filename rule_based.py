import Levenshtein as ls
from py_stringmatching import GeneralizedJaccard, Soundex

from data import Row

def rule_based_ssn(row1, row2):
    ssn1 = row1.__getitem__('ssn')
    ssn2 = row2.__getitem__('ssn')
    if ssn1 == '000000000' or ssn2 == '000000000':
        return -1
    ratio = ls.ratio(ssn1, ssn2)
    if ratio <= 0.555:
        return 0
    else:
        return ratio

def rule_based_name(row1, row2, percentage):
    name1 = [row1.__getitem__('fname'), row1.__getitem__('lname')]
    if len(row1.__getitem__('minit')) != 0:
        name1.append(row1.__getitem__('minit'))
    name2 = [row2.__getitem__('fname'), row2.__getitem__('lname')]
    if len(row2.__getitem__('minit')) != 0:
        name2.append(row1.__getitem__('minit'))
    gj = GeneralizedJaccard(sim_func=Soundex().get_raw_score)
    return gj.get_sim_score(name1, name2) * percentage

def rule_based_stadd(row1, row2, percentage):
    attr1 = row1.__getitem__('stadd').split(' ')[0]
    attr2 = row2.__getitem__('stadd').split(' ')[0]
    return ls.ratio(attr1, attr2) * percentage

def rule_based_other_attributes(row1, row2, attr_name, percentage):
    attr1 = row1.__getitem__(attr_name)
    attr2 = row2.__getitem__(attr_name)
    return ls.ratio(attr1, attr2) * percentage

def rule_based_method(row1, row2):
    attr_percentage = {
        'name': 0.2,
        'stnum': 0.032,
        'stadd': 0.041,
        'apmt': 0.048,
        'city': 0.063,
        'state': 0.058,
        'zip': 0.058
    }
    ssn_ratio = rule_based_ssn(row1, row2)
    if ssn_ratio == 1:
        return True
    elif ssn_ratio == 0:
        return False
    else:
        result = rule_based_name(row1, row2, attr_percentage.get('name'))
        result += rule_based_other_attributes(row1, row2, 'stnum', attr_percentage.get('stnum'))
        result += rule_based_stadd(row1, row2, attr_percentage.get('stadd'))
        result += rule_based_other_attributes(row1, row2, 'apmt', attr_percentage.get('apmt'))
        result += rule_based_other_attributes(row1, row2, 'apmt', attr_percentage.get('city'))
        result += rule_based_other_attributes(row1, row2, 'apmt', attr_percentage.get('state'))
        result += rule_based_other_attributes(row1, row2, 'apmt', attr_percentage.get('zip'))
        if ssn_ratio == -1:
            result *= 2
            return result >= 0.691
        else:
            result += ssn_ratio * 0.5
            return result >= 0.788

row1 = Row(0, 0, ('485138724', 'Kuzn', 'W', 'Chiun', '975', 'Lonni Avenue', '6t6', 'Waco', 'TX', '76708'))
row2 = Row(5, 1, ('143034838', 'Blaik', 'Y', 'Mcelderry', '253', 'Klement Avenue', '8a7', 'Little Rock', 'AR', '72205'))
row3 = Row(0, 757676, ('485138724', 'Cghiun', 'W', 'Kuzn', '475', 'Lonin Av', '6t6', 'Wao', 'FM', '76408'))
row4 = Row(0, 757676, ('000000000', 'Cghiun', 'W', 'Kuzn', '475', 'Lonin Av', '6t6', 'Wao', 'FM', '76408'))

print(rule_based_method(row1, row2))
print(rule_based_method(row1, row3))
print(rule_based_method(row1, row4))
