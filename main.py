from Classifier.eval_classifier import main
from meta_blocking import *
from data import Dataset


def block_main():
    dataset = Dataset.from_csv('data/0-5000#5000-10000/train.csv')
    blocking = MultiBlocking([
        (SoundexBlocking(), 'fname', 0),
        (SoundexBlocking(), 'lname', 0)
    ])
    blocking = SoundexBlocking()
    print(len(dataset.rows))
    blocks = blocking(dataset, 'fname')
    print([len(x) for x in blocks])
    print(blocks)



if __name__ == '__main__':
    block_main()
