# coding=utf-8
# Copyright 2022 Runlong He, Beihang University.
# Copyright 2020 Jiaming Shen, University of Illinois at Urbana-Champaign, Data Mining Group.
# Copyright 2019 Hao WANG, Shanghai University, KB-NLP team.
# Copyright 2018 The Google AI Language Team Authors and The HuggingFace Inc. team.
# Copyright (c) 2018, NVIDIA CORPORATION.  All rights reserved.

from __future__ import absolute_import, division, print_function

import csv
import logging
import os
import sys
from io import open
import math

from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import accuracy_score

logger = logging.getLogger(__name__)

# Used for SemEval dataset
SEMEVAL_RELATION_LABELS = ['Other', 'Message-Topic(e1,e2)', 'Message-Topic(e2,e1)',
                           'Product-Producer(e1,e2)', 'Product-Producer(e2,e1)',
                           'Instrument-Agency(e1,e2)', 'Instrument-Agency(e2,e1)',
                           'Entity-Destination(e1,e2)', 'Entity-Destination(e2,e1)',
                           'Cause-Effect(e1,e2)', 'Cause-Effect(e2,e1)',
                           'Component-Whole(e1,e2)', 'Component-Whole(e2,e1)',
                           'Entity-Origin(e1,e2)', 'Entity-Origin(e2,e1)',
                           'Member-Collection(e1,e2)', 'Member-Collection(e2,e1)',
                           'Content-Container(e1,e2)', 'Content-Container(e2,e1)']


class InputExample(object):
    """A single training/test example for simple sequence classification."""

    def __init__(self, guid, text_a, text_b=None, label=None):  # InputExample(guid=guid, text_a=text_a, text_b=text_b, label=label)
        """Constructs a InputExample.
        Args:
            guid: Unique id for the example.
            text_a: string. The untokenized text of the first sequence. For single
            sequence tasks, only this sequence must be specified.
            text_b: (Optional) string. The untokenized text of the second sequence.
            Only must be specified for sequence pair tasks.
            label: (Optional) string. The label of the example. This should be
            specified for train and dev examples, but not for test examples.
        """
        self.guid = guid
        self.text_a = text_a
        self.text_b = text_b
        self.label = label


class InputFeatures(object):
    """A single set of features of data."""

    def __init__(self,
                 input_ids,
                 input_mask,
                 e11_p, e12_p, e21_p, e22_p,
                 e1_mask, e2_mask,
                 segment_ids,
                 label_id):
        self.input_ids = input_ids
        self.input_mask = input_mask
        self.segment_ids = segment_ids
        self.label_id = label_id
        self.e11_p = e11_p
        self.e12_p = e12_p
        self.e21_p = e21_p
        self.e22_p = e22_p
        self.e1_mask = e1_mask
        self.e2_mask = e2_mask


class DataProcessor(object):
    """Base class for data converters for sequence classification data sets."""

    def get_train_examples(self, data_dir):
        """Gets a collection of `InputExample`s for the train set."""
        raise NotImplementedError()

    def get_dev_examples(self, data_dir):
        """Gets a collection of `InputExample`s for the dev set."""
        raise NotImplementedError()

    def get_labels(self):
        """Gets the list of labels for this data set."""
        raise NotImplementedError()

    @classmethod
    def _read_tsv(cls, input_file, quotechar=None):
        """Reads a tab separated value file."""
        with open(input_file, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter="\t", quotechar=quotechar)
            lines = []
            for line in reader:
                if sys.version_info[0] == 2:
                    line = list(cell for cell in line)
                lines.append(line)
            return lines


class SemEvalProcessor(DataProcessor):
    """Processor for the SemEval-2010 data set."""

    def get_train_examples(self, data_dir):
        """See base class."""
        logger.info("LOOKING AT {}".format(os.path.join(data_dir, "train_dataset.tsv")))
        return self._create_examples(
            self._read_tsv(os.path.join(data_dir, "train_dataset.tsv")), "train")

    def get_dev_examples(self, data_dir):
        """
        See base class.
        关系模型的输入，修改"eval_dataset.tsv"为输入文件名即可
        """
        return self._create_examples(
            self._read_tsv(os.path.join(data_dir, "eval_dataset.tsv")), "dev")

    def get_labels(self):  # 创建标签对应的ID, SemEval有2*9+1=19种关系
        """See base class."""
        return [str(i) for i in range(19)]

    def _create_examples(self, lines, set_type):  # 创建数据实例
        """Creates examples for the training and dev sets.
        e.g.,: 
        2	the [E11] author [E12] of a keygen uses a [E21] disassembler [E22] to look at the raw assembly code .	6
        """
        # print(lines)  # [..., ['7999', 'the [E11] surgeon [E12] cuts a small [E21] hole [E22] in the skull ... the nerve', '4', 'producer', 'product', '2']]
        # print(set_type)  # 'train' or 'dev'
        examples = []
        for (i, line) in enumerate(lines):
            # print(line[0])
            guid = "%s-%s" % (set_type, i)
            text_a = line[1]  # 句子文本
            text_b = None
            label = line[2]  # 第三列，'4' (第四列和第五列并没有使用)
            examples.append(
                InputExample(guid=guid, text_a=text_a, text_b=text_b, label=label))
        return examples


def convert_examples_to_features(examples, label_list, max_seq_len,  # 将句子转换成符合BERT输入规范的内容
                                 tokenizer, output_mode,
                                 cls_token='[CLS]',
                                 cls_token_segment_id=1,
                                 sep_token='[SEP]',
                                 pad_token=0,
                                 pad_token_segment_id=0,
                                 sequence_a_segment_id=0,
                                 sequence_b_segment_id=1,
                                 mask_padding_with_zero=True,
                                 use_entity_indicator=True):
    """ Loads a data file into a list of `InputBatch`s
        Default, BERT/XLM pattern: [CLS] + A + [SEP] + B + [SEP]
        `cls_token_segment_id` define the segment id associated to the CLS token (0 for BERT, 2 for XLNet)
    """  # 转换为"[CLS] + A + [SEP] + B + [SEP]"列表

    label_map = {label: i for i, label in enumerate(label_list)}
    # print(label_map)  # {'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, ..., '18': 18}

    features = []
    for (ex_index, example) in enumerate(examples):
        if ex_index % 10000 == 0:
            logger.info("Writing example %d of %d" % (ex_index, len(examples)))

        tokens_a = tokenizer.tokenize(example.text_a)
        # print(tokens_a)  # ['the', 'system', 'as', 'described', 'above', 'has', 'its', 'greatest', 'application', 'in', 'an', 'array', '##ed', '[E11]', 'configuration', '[E12]', 'of', 'antenna', '[E21]', 'elements', '[E22]']
        tokens_b = None

        if example.text_b:
            tokens_b = tokenizer.tokenize(example.text_b)
            # Modifies `tokens_a` and `tokens_b` in place so that the total
            # length is less than the specified length.
            # Account for [CLS], [SEP], [SEP] with "- 3". " -4" for RoBERTa.
            special_tokens_count = 3
            _truncate_seq_pair(tokens_a, tokens_b,  # 控制总长度为max_seq_len(包含特殊符号) 但为什么是128？512也可以吧
                               max_seq_len - special_tokens_count)
        else:
            # Account for [CLS] and [SEP] with "- 2" and with "- 3" for RoBERTa.
            special_tokens_count = 2
            if len(tokens_a) > max_seq_len - special_tokens_count:  # 如果超长, 进行截断
                tokens_a = _truncate_seq(tokens_a, max_seq_len - special_tokens_count)
                
        # The convention in BERT is:  # BERT的习俗
        # (a) For sequence pairs:
        #  tokens:   [CLS] is this jack ##son ##ville ? [SEP] no it is not . [SEP]
        #  type_ids:   0   0  0    0    0     0       0   0   1  1  1  1   1   1
        # (b) For single sequences:
        #  tokens:   [CLS] the dog is hairy . [SEP]
        #  type_ids:   0   0   0   0  0     0   0
        #
        # Where "type_ids" are used to indicate whether this is the first
        # sequence or the second sequence. The embedding vectors for `type=0` and
        # `type=1` were learned during pre-training and are added to the wordpiece
        # embedding vector (and position vector). This is not *strictly* necessary
        # since the [SEP] token unambiguously separates the sequences, but it makes
        # it easier for the model to learn the concept of sequences.
        #
        # For classification tasks, the first vector (corresponding to [CLS]) is
        # used as the "sentence vector". Note that this only makes sense because
        # the entire model is fine-tuned.
        tokens = tokens_a + [sep_token]  # 在句子后面加[SEP]
        segment_ids = [sequence_a_segment_id] * len(tokens)  # 全是0

        if tokens_b:
            tokens += tokens_b + [sep_token]
            segment_ids += [sequence_b_segment_id] * (len(tokens_b) + 1)

        tokens = [cls_token] + tokens  # 在句子前面加[CLS]
        segment_ids = [cls_token_segment_id] + segment_ids

        input_ids = tokenizer.convert_tokens_to_ids(tokens)  # 输入序列的id

        # entity mask
        if use_entity_indicator:
            head = 0
            tail = 0
            for token in tokens:
                if "[E22:" in token:
                    tail = 1
                elif "[E12:" in token:
                    head = 1
                else:
                    continue
            if tail == 0 or head == 0:
                logger.warning(f"*** Example-{ex_index} is skipped ***")  # 如果实体对因截断而不完整，则略过该样本句
                continue
            else:
                e11_p = 0
                e12_p = 0
                e21_p = 0
                e22_p = 0
                for i, token in enumerate(tokens):
                    if '[E11:' in token:
                        e11_p = i + 1
                    elif '[E12:' in token:
                        e12_p = i
                    elif '[E21:' in token:
                        e21_p = i + 1
                    elif '[E22:' in token:
                        e22_p = i
                    else:
                        continue

        # The mask has 1 for real tokens and 0 for padding tokens. Only real tokens are attended to.
        input_mask = [1 if mask_padding_with_zero else 0] * len(input_ids)

        # Zero-pad up to the sequence length.
        padding_length = max_seq_len - len(input_ids)
        input_ids = input_ids + ([pad_token] * padding_length)  # 补0
        input_mask = input_mask + ([0 if mask_padding_with_zero else 1] * padding_length)
        segment_ids = segment_ids + ([pad_token_segment_id] * padding_length)
        if use_entity_indicator:
            e1_mask = [0 for i in range(len(input_mask))]
            e2_mask = [0 for i in range(len(input_mask))]
            for i in range(e11_p, e12_p):
                e1_mask[i] = 1
            for i in range(e21_p, e22_p):
                e2_mask[i] = 1

        assert len(input_ids) == max_seq_len, f"Error in sample: {ex_index}, len(input_ids)={len(input_ids)}"
        assert len(input_mask) == max_seq_len
        assert len(segment_ids) == max_seq_len

        if output_mode == "classification":
            # label_id = label_map[example.label]
            # print(example.text_a)
            # print(example.label)
            label_id = int(example.label)  # from str to int
        elif output_mode == "regression":
            label_id = float(example.label)
        else:
            raise KeyError(output_mode)

        if ex_index < 5:
            logger.info("*** Example ***")
            logger.info("guid: %s" % (example.guid))
            logger.info("tokens: %s" % " ".join(
                [str(x) for x in tokens]))
            logger.info("input_ids: %s" %
                        " ".join([str(x) for x in input_ids]))
            logger.info("input_mask: %s" %
                        " ".join([str(x) for x in input_mask]))
            if use_entity_indicator:
                logger.info("e11_p: %s" % e11_p)
                logger.info("e12_p: %s" % e12_p)
                logger.info("e21_p: %s" % e21_p)
                logger.info("e22_p: %s" % e22_p)
                logger.info("e1_mask: %s" %
                            " ".join([str(x) for x in e1_mask]))
                logger.info("e2_mask: %s" %
                            " ".join([str(x) for x in e2_mask]))
            logger.info("segment_ids: %s" %
                        " ".join([str(x) for x in segment_ids]))
            logger.info("label: %s (id = %d)" % (example.label, label_id))

        features.append(
            InputFeatures(input_ids=input_ids,
                          input_mask=input_mask,
                          e11_p=e11_p,
                          e12_p=e12_p,
                          e21_p=e21_p,
                          e22_p=e22_p,
                          e1_mask=e1_mask,
                          e2_mask=e2_mask,
                          segment_ids=segment_ids,
                          label_id=label_id))

    return features


def _truncate_seq_pair(tokens_a, tokens_b, max_length):
    """Truncates a sequence pair in place to the maximum length."""

    # This is a simple heuristic which will always truncate the longer sequence
    # one token at a time. This makes more sense than truncating an equal percent
    # of tokens from each, since if one sequence is very short then each token
    # that's truncated likely contains more information than a longer sequence.
    while True:
        total_length = len(tokens_a) + len(tokens_b)
        if total_length <= max_length:
            break
        if len(tokens_a) > len(tokens_b):  # 谁长对谁进行截断
            tokens_a.pop()
        else:
            tokens_b.pop()


def _truncate_seq(tokens_a, max_length):
    """Truncates a sequence """
    tmp = tokens_a[:max_length]
    head = 0
    tail = 0
    for token in tmp:
        if '[E12:' in token:
            head = 1
        elif '[E22:' in token:
            tail = 1
        else:
            continue
    if head == 1 and tail == 1:
        return tmp
    else:  # 修改特殊符号的话，这里也要改
        e11_p = 0
        e12_p = 0
        e21_p = 0
        e22_p = 0
        for i, token in enumerate(tokens_a):
            if '[E11:' in token:
                e11_p = i
            elif '[E12:' in token:
                e12_p = i
            elif '[E21:' in token:
                e21_p = i
            elif '[E22:' in token:
                e22_p = i
            else:
                continue
        start = min(e11_p, e12_p, e21_p, e22_p)
        end = max(e11_p, e12_p, e21_p, e22_p)
        if end-start > max_length:
            remaining_length = max_length - (e12_p-e11_p+1) - (e22_p-e21_p+1)  
            first_addback = math.floor(remaining_length/2)
            second_addback = remaining_length - first_addback
            if start == e11_p:
                new_tokens = tokens_a[e11_p: e12_p+1+first_addback] + tokens_a[e21_p-second_addback:e22_p+1]
            else:
                new_tokens = tokens_a[e21_p: e22_p+1+first_addback] + tokens_a[e11_p-second_addback:e12_p+1]
            return new_tokens
        else:
            new_tokens = tokens_a[start:end+1]
            remaining_length = max_length - len(new_tokens)
            if start < remaining_length:  # add sentence beginning back
                new_tokens = tokens_a[:start] + new_tokens 
                remaining_length -= start
            else:
                new_tokens = tokens_a[start-remaining_length:start] + new_tokens
                return new_tokens

            # still some room left, add sentence end back
            new_tokens = new_tokens + tokens_a[end+1:end+1+remaining_length]
            return new_tokens


def simple_accuracy(preds, labels):
    return (preds == labels).mean()


def acc_and_f1(preds, labels, average='macro'):
    # acc = simple_accuracy(preds, labels)
    acc = accuracy_score(y_true=labels, y_pred=preds)
    precision = precision_score(y_true=labels, y_pred=preds, average=average)
    recall = recall_score(y_true=labels, y_pred=preds, average=average)
    f1 = f1_score(y_true=labels, y_pred=preds, average=average)
    return {
        "acc": acc,
        "precision": precision,
        "recall": recall,
        "f1": f1
    }


def compute_metrics(task_name, preds, labels):
    assert len(preds) == len(labels)
    return acc_and_f1(preds, labels)


data_processors = {
    "semeval": SemEvalProcessor,
}

output_modes = {
    "semeval": "classification",
}

GLUE_TASKS_NUM_LABELS = {
    "semeval": 19,
}
