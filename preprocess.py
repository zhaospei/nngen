import argparse
import re
import pandas as pd
import random

import nltk
from nltk import WordNetLemmatizer, pos_tag, WordPunctTokenizer, data
from nltk.corpus import wordnet

def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    if treebank_tag.startswith('V'):
        return wordnet.VERB
    if treebank_tag.startswith('N'):
        return wordnet.NOUN
    if treebank_tag.startswith('R'):
        return wordnet.ADV
    return wordnet.NOUN


def starts_with_verb(word_list):
    if len(word_list) <= 0:
        return False
    word_list = ['He'] + word_list
    count = 0
    for word, pos in pos_tag(word_list):
        treebank_tag = get_wordnet_pos(pos)
        if count == 1:
            return treebank_tag.startswith('V') or treebank_tag.startswith('v')
        count += 1
def write_string_to_file(absolute_filename, string):
    with open(absolute_filename, 'w') as fout:
        fout.write(string)

def split_data(source_seqs, target_seqs):
    TEST_RATE = 0.12
    EVAL_RATE = 0.15 + TEST_RATE 
    train_source_seqs = list()
    train_target_seqs = list()
    valid_source_seqs = list()
    valid_target_seqs = list()
    test_source_seqs = list()
    test_target_seqs = list()
  
    for idx, source_seq in enumerate(source_seqs):
        target_seq = target_seqs[idx]
        random_prob = random.random()
        if random_prob < TEST_RATE:
            test_source_seqs.append(source_seq)
            test_target_seqs.append(target_seq)
        elif random_prob < EVAL_RATE:
            valid_source_seqs.append(source_seq)
            valid_target_seqs.append(target_seq)
        else:
            train_source_seqs.append(source_seq)
            train_target_seqs.append(target_seq)
    
    write_string_to_file(f'data/cmg.test.diff', '\n'.join(test_source_seqs))
    write_string_to_file(f'data/cmg.test.msg', '\n'.join(test_target_seqs))
    write_string_to_file(f'data/cmg.train.diff', '\n'.join(train_source_seqs))
    write_string_to_file(f'data/cmg.train.msg', '\n'.join(train_target_seqs))
    write_string_to_file(f'data/cmg.valid.diff', '\n'.join(valid_source_seqs))
    write_string_to_file(f'data/cmg.valid.msg', '\n'.join(valid_target_seqs))

def word_tokenizer(sentence):
    words = WordPunctTokenizer().tokenize(sentence)
    return words

def read_raw_data(file):
    print(file)
    df = pd.read_parquet(file, engine='fastparquet')
    source_seqs = list()
    target_seqs = list()

    for _, row in df.iterrows():
        diffs = list()
        for l in row['diff'].splitlines():
            l = re.sub('@@.+?@@', '', l)
            l = re.sub(r'\s+', ' ', l)
            if len(l) <= 0:
                continue
            words = word_tokenizer(l)
            diffs.append(' '.join(words))
        source_seq = ' <nl> '.join(diffs)
        label_words = word_tokenizer(row['label'])
        target_seq = ' '.join(label_words)
        source_words = [word for word in source_seq.split()]
        target_words = [word for word in target_seq.split()]
        
        if len(source_words) > 100:
            continue

        if len(target_words) > 30 or not starts_with_verb(target_words):
            continue
        
        source_seqs.append(source_seq)
        target_seqs.append(target_seq)
    
    print(len(source_seqs))
    split_data(source_seqs, target_seqs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_dir", default=None, type=str,
                        help="File dir to read raw data")
    
    args = parser.parse_args()

    read_raw_data(args.file_dir)


if __name__ == "__main__":
    main()
