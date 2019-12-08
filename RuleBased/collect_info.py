import csv
import numpy as np
import random
import Levenshtein as ls
from py_stringmatching import GeneralizedJaccard, Soundex

from data import Dataset

class EquivalenceClasses:
    def __init__(self, path):
        self.dataset = Dataset('train.csv')
        self.contents = []
        self.gj = GeneralizedJaccard(sim_func=Soundex().get_raw_score)

        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            contents = []
            for line in reader:
                elements = []
                for element in line[1:]:
                    elements.append(self.dataset.rows[int(element)])
                contents.append(elements)
            self.contents = [content for content in contents if len(content) != 0]

    def calcEquivalentSsn(self):
        print('ssn-eq')
        ssn_info = []
        for content in self.contents:
            for i in range(len(content) - 2):
                ssn1 = content[i].__getitem__('ssn')
                if ssn1 != '000000000':
                    for j in range(i + 1, len(content) - 1):
                        ssn2 = content[j].__getitem__('ssn')
                        if ssn2 != '000000000':
                            ssn_info.append(ls.ratio(ssn1, ssn2))
        narray = np.array(ssn_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def calcInequivalentSsn(self):
        print('ssn-neq')
        ssn_info = []
        random_array = self.generateRandomArray(len(self.contents) - 2, 100)
        for i in random_array:
            ssn1 = self.contents[i][0].__getitem__('ssn')
            if ssn1 != '000000000':
                for j in range(i + 1, len(self.contents) - 1):
                    ssn2 = self.contents[j][0].__getitem__('ssn')
                    if ssn2 != '000000000':
                        ssn_info.append(ls.ratio(ssn1, ssn2))
        narray = np.array(ssn_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def calcEquivalentName(self):
        print('name-eq')
        name_info = []
        
        for content in self.contents:
            for i in range(len(content) - 2):
                name1 = [content[i].__getitem__('fname'), content[i].__getitem__('lname')]
                if len(content[i].__getitem__('minit')) != 0:
                    name1.append(content[i].__getitem__('minit'))
                for j in range(i + 1, len(content) - 1):
                    name2 = [content[j].__getitem__('fname'), content[j].__getitem__('lname')]
                    if len(content[j].__getitem__('minit')) != 0:
                        name2.append(content[j].__getitem__('minit'))
                    name_info.append(self.gj.get_sim_score(name1, name2))
        narray = np.array(name_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def calcInequivalentName(self):
        print('name-neq')
        name_info = []
        random_array = self.generateRandomArray(len(self.contents) - 2, 10)
        print(random_array)
        for i in random_array:
            name1 = [self.contents[i][0].__getitem__('fname'), self.contents[i][0].__getitem__('lname')]
            if len(self.contents[i][0].__getitem__('minit')) != 0:
                name1.append(self.contents[i][0].__getitem__('minit'))
            print(name1)
            for j in range(i + 1, len(self.contents) - 1):
                name2 = [self.contents[j][0].__getitem__('fname'), self.contents[j][0].__getitem__('lname')]
                if len(self.contents[j][0].__getitem__('minit')) != 0:
                    name2.append(self.contents[j][0].__getitem__('minit'))
                name_info.append(self.gj.get_sim_score(name1, name2))
        narray = np.array(name_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def calcEquivalentStadd(self):
        print('stadd-eq')
        stadd_info = []
        for content in self.contents:
            for i in range(len(content) - 2):
                stadd1 = content[i].__getitem__('stadd').split(' ')[0]
                for j in range(i + 1, len(content) - 1):
                    stadd2 = content[j].__getitem__('stadd').split(' ')[0]
                    stadd_info.append(ls.ratio(stadd1, stadd2))
        narray = np.array(stadd_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def calcInequivalentStadd(self):
        print('stadd-neq')
        stadd_info = []
        random_array = self.generateRandomArray(len(self.contents) - 2, 100)
        for i in random_array:
            stadd1 = self.contents[i][0].__getitem__('stadd').split(' ')[0]
            for j in range(i + 1, len(self.contents) - 1):
                stadd2 = self.contents[j][0].__getitem__('stadd').split(' ')[0]
                stadd_info.append(ls.ratio(stadd1, stadd2))
        narray = np.array(stadd_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def calcEquivalentOtherAttributes(self, attr_name):
        print(attr_name + '-eq')
        attr_info = []
        for content in self.contents:
            for i in range(len(content) - 2):
                attr1 = content[i].__getitem__(attr_name)
                for j in range(i + 1, len(content) - 1):
                    attr2 = content[j].__getitem__(attr_name)
                    attr_info.append(ls.ratio(attr1, attr2))
        narray = np.array(attr_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def calcInequivalentOtherAttributes(self, attr_name):
        print(attr_name + '-neq')
        attr_info = []
        random_array = self.generateRandomArray(len(self.contents) - 2, 100)
        for i in random_array:
            attr1 = self.contents[i][0].__getitem__(attr_name)
            for j in range(i + 1, len(self.contents) - 1):
                attr2 = self.contents[j][0].__getitem__(attr_name)
                attr_info.append(ls.ratio(attr1, attr2))
        narray = np.array(attr_info)
        print('mean: ' + str(np.mean(narray)))
        print('max: ' + str(np.max(narray)))
        print('min: ' + str(np.min(narray)))

    def generateRandomArray(self, max_value, number):
        random_array = []
        for i in range(0, number):
            random_array.append(random.randint(0, max_value))
        return set(random_array)

def main():
    equivalenceClasses = EquivalenceClasses('bm_real.csv')

    equivalenceClasses.calcEquivalentSsn()
    equivalenceClasses.calcInequivalentSsn()

    equivalenceClasses.calcEquivalentName()
    equivalenceClasses.calcInequivalentName()

    equivalenceClasses.calcEquivalentOtherAttributes('stnum')
    equivalenceClasses.calcInequivalentOtherAttributes('stnum')

    equivalenceClasses.calcEquivalentStadd()
    equivalenceClasses.calcInequivalentStadd()

    equivalenceClasses.calcEquivalentOtherAttributes('apmt')
    equivalenceClasses.calcInequivalentOtherAttributes('apmt')

    equivalenceClasses.calcEquivalentOtherAttributes('city')
    equivalenceClasses.calcInequivalentOtherAttributes('city')

    equivalenceClasses.calcEquivalentOtherAttributes('state')
    equivalenceClasses.calcInequivalentOtherAttributes('state')

    equivalenceClasses.calcEquivalentOtherAttributes('zip')
    equivalenceClasses.calcInequivalentOtherAttributes('zip')

if __name__ == '__main__':
    main()
