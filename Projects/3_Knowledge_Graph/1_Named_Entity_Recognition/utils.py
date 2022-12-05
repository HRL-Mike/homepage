# -*- coding: utf-8 -*-

'''
Description: This file is for implementing Dataset. 
'''

import torch
from torch.utils.data import Dataset
from transformers import BertTokenizer

BERT_PATH = r'C:\Users\HRL\Desktop\毕业设计\代码\实体抽取代码\BERT+双向LSTM+CRF\bert-base-cased'
tokenizer = BertTokenizer.from_pretrained(BERT_PATH)
VOCAB = ('<PAD>', '[CLS]', '[SEP]', 'O', "B-attacker", "I-attacker", "B-malware", "I-malware",
         "B-vulnerability", "B-domain", "B-ip", "B-industry", "I-industry", "B-technique", "I-technique",
         "B-location", "I-location", "B-tool", "I-tool")

tag2idx = {tag: idx for idx, tag in enumerate(VOCAB)}
idx2tag = {idx: tag for idx, tag in enumerate(VOCAB)}
MAX_LEN = 128-2  # 256 -> 128


class NerDataset(Dataset):
    ''' Generate our dataset '''
    def __init__(self, f_path):
        self.sents = []
        self.tags_li = []

        with open(f_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        words, tags = [], []
        for line in lines:
            if line != '\n':
                words.append(line.strip('\n').split(' ')[0])
                tags.append(line.strip('\n').split(' ')[1])
            else:   # 如果是句号
                if len(words) > MAX_LEN:
                    while len(words) > MAX_LEN:  # 将长句按最大长度(128)进行切分
                        self.sents.append(['[CLS]'] + words[:MAX_LEN] + ['[SEP]'])
                        self.tags_li.append(['[CLS]'] + tags[:MAX_LEN] + ['[SEP]'])
                        words = words[MAX_LEN:]
                        tags = tags[MAX_LEN:]
                    self.sents.append(['[CLS]'] + words[:MAX_LEN] + ['[SEP]'])
                    self.tags_li.append(['[CLS]'] + tags[:MAX_LEN] + ['[SEP]'])
                else:
                    self.sents.append(['[CLS]'] + words + ['[SEP]'])
                    self.tags_li.append(['[CLS]'] + tags + ['[SEP]'])
                words, tags = [], []

    def __getitem__(self, idx):
        words, tags = self.sents[idx], self.tags_li[idx]
        token_ids = tokenizer.convert_tokens_to_ids(words)
        laebl_ids = [tag2idx[tag] for tag in tags]
        seqlen = len(laebl_ids)
        return token_ids, laebl_ids, seqlen

    def __len__(self):
        return len(self.sents)


def PadBatch(batch):
    maxlen = max([i[2] for i in batch])
    token_tensors = torch.LongTensor([i[0] + [0] * (maxlen - len(i[0])) for i in batch])
    label_tensors = torch.LongTensor([i[1] + [0] * (maxlen - len(i[1])) for i in batch])
    mask = (token_tensors > 0)
    return token_tensors, label_tensors, mask
