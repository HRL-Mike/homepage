# -*- coding: utf-8 -*-

import torch
import torch.nn as nn
from transformers import BertModel
from torchcrf import CRF
from utils import BERT_PATH


class Bert_BiLSTM_CRF(nn.Module):

    def __init__(self, tag_to_ix, embedding_dim=768, hidden_dim=256):
        super(Bert_BiLSTM_CRF, self).__init__()
        self.tag_to_ix = tag_to_ix
        self.tagset_size = len(tag_to_ix)
        self.hidden_dim = hidden_dim
        self.embedding_dim = embedding_dim

        self.bert = BertModel.from_pretrained(BERT_PATH, return_dict=False)  # 解决last_hidden_state是str的问题
        self.lstm = nn.LSTM(input_size=embedding_dim, hidden_size=hidden_dim//2,
                            num_layers=2, bidirectional=True, batch_first=True)
        self.dropout = nn.Dropout(p=0.1)
        self.linear = nn.Linear(hidden_dim, self.tagset_size)
        self.crf = CRF(self.tagset_size, batch_first=True)
    
    def _get_features(self, sentence):
        with torch.no_grad():
            embeds, _ = self.bert(sentence)
        # print(type(self.bert))  # <class 'transformers.models.bert.modeling_bert.BertModel'>
        # print(type(embeds))  # last_hidden_state str
        # print(type(_))  # pooler_output str
        enc, _ = self.lstm(embeds)  # 'str' object has no attribute 'size'
        enc = self.dropout(enc)
        feats = self.linear(enc)
        return feats

    def forward(self, sentence, tags, mask, is_test=False):
        emissions = self._get_features(sentence)
        if not is_test:  # Training，return loss
            loss = -self.crf.forward(emissions, tags, mask, reduction='mean')
            return loss
        else:  # Testing，return decoding
            decode = self.crf.decode(emissions, mask)
            return decode
