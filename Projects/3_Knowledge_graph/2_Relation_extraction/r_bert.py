# -*- coding:utf-8 -*-

import glob
import logging
import os
import random
import numpy as np
import torch

from torch.utils.data import (DataLoader, RandomSampler, SequentialSampler, TensorDataset)
from torch.utils.data.distributed import DistributedSampler
from tqdm import tqdm, trange
from transformers import (WEIGHTS_NAME, BertConfig, BertTokenizer)
from transformers import AdamW, get_linear_schedule_with_warmup
from utils import (SEMEVAL_RELATION_LABELS, compute_metrics,
                   convert_examples_to_features, data_processors)

from argparse import ArgumentParser
from config import Config
from model import BertForSequenceClassification

logger = logging.getLogger(__name__)
additional_special_tokens = ["[E11]", "[E12]", "[E21]", "[E22]"]
additional_special_tokens_2 = ["[E11:attacker]", "[E12:attacker]", "[E21:attacker]", "[E22:attacker]",
                               "[E11:malware]", "[E12:malware]", "[E21:malware]", "[E22:malware]",
                               "[E11:vulnerability]", "[E12:vulnerability]", "[E21:vulnerability]", "[E22:vulnerability]",
                               "[E11:domain]", "[E12:domain]", "[E21:domain]", "[E22:domain]",
                               "[E11:ip]", "[E12:ip]", "[E21:ip]", "[E22:ip]",
                               "[E11:industry]", "[E12:industry]", "[E21:industry]", "[E22:industry]",
                               "[E11:technique]", "[E12:technique]", "[E21:technique]", "[E22:technique]",
                               "[E11:location]", "[E12:location]", "[E21:location]", "[E22:location]",
                               "[E11:tool]", "[E12:tool]", "[E21:tool]", "[E22:tool]"]


def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def train(config, train_dataset, model, tokenizer):
    """ Train the model """
    config.train_batch_size = config.per_gpu_train_batch_size * \
        max(1, config.n_gpu)
    if config.local_rank == -1:
        train_sampler = RandomSampler(train_dataset)
    else:
        DistributedSampler(train_dataset)

    train_dataloader = DataLoader(
        train_dataset, sampler=train_sampler, batch_size=config.train_batch_size)

    if config.max_steps > 0:
        t_total = config.max_steps
        config.num_train_epochs = config.max_steps // (
            len(train_dataloader) // config.gradient_accumulation_steps) + 1
    else:
        t_total = len(
            train_dataloader) // config.gradient_accumulation_steps * config.num_train_epochs

    # Prepare optimizer and schedule (linear warmup and decay)
    no_decay = ['bias', 'LayerNorm.weight']
    optimizer_grouped_parameters = [
        {'params': [p for n, p in model.named_parameters()
                    if not any(nd in n for nd in no_decay)], 'weight_decay': config.weight_decay},
        {'params': [p for n, p in model.named_parameters()
                    if any(nd in n for nd in no_decay)], 'weight_decay': 0.0}
    ]
    optimizer = AdamW(optimizer_grouped_parameters,
                      lr=config.learning_rate, eps=config.adam_epsilon)
    scheduler = get_linear_schedule_with_warmup(
        optimizer, num_warmup_steps=config.warmup_steps, num_training_steps=t_total)
    if config.n_gpu > 1:
        model = torch.nn.DataParallel(model)

    # Distributed training (should be after apex fp16 initialization)
    if config.local_rank != -1:
        model = torch.nn.parallel.DistributedDataParallel(model, device_ids=[config.local_rank],
                                                          output_device=config.local_rank,
                                                          find_unused_parameters=True)

    # Train!
    logger.info("***** Running training *****")
    logger.info("  Num examples = %d", len(train_dataset))
    logger.info("  Num Epochs = %d", config.num_train_epochs)
    logger.info("  Instantaneous batch size per GPU = %d",
                config.per_gpu_train_batch_size)
    logger.info("  Total train batch size (w. parallel, distributed & accumulation) = %d",
                config.train_batch_size * config.gradient_accumulation_steps
                * (torch.distributed.get_world_size() if config.local_rank != -1 else 1))
    logger.info("  Gradient Accumulation steps = %d",
                config.gradient_accumulation_steps)
    logger.info("  Total optimization steps = %d", t_total)

    global_step = 0
    tr_loss, logging_loss = 0.0, 0.0
    model.zero_grad()
    train_iterator = trange(int(config.num_train_epochs),
                            desc="Epoch", disable=config.local_rank not in [-1, 0])
    # Added here for reproductibility (even between python 2 and 3)
    set_seed(config.seed)
    for _ in train_iterator:
        epoch_iterator = tqdm(train_dataloader, desc="Iteration",
                              disable=config.local_rank not in [-1, 0])
        for step, batch in enumerate(epoch_iterator):
            model.train()
            batch = tuple(t.to(config.device) for t in batch)
            inputs = {'input_ids':      batch[0],
                      'attention_mask': batch[1],
                      # XLM and RoBERTa don't use segment_ids
                      'token_type_ids': batch[2],
                      'labels':      batch[3],
                      'e1_mask': batch[4],
                      'e2_mask': batch[5],
                      }

            outputs = model(**inputs)
            # model outputs are always tuple in pytorch-transformers (see doc)
            loss = outputs[0]
            if config.n_gpu > 1:
                loss = loss.mean()  # mean() to average on multi-gpu parallel training
            if config.gradient_accumulation_steps > 1:
                loss = loss / config.gradient_accumulation_steps

            loss.backward()
            torch.nn.utils.clip_grad_norm_(
                model.parameters(), config.max_grad_norm)

            tr_loss += loss.item()
            if (step + 1) % config.gradient_accumulation_steps == 0:

                optimizer.step()
                scheduler.step()  # Update learning rate schedule
                model.zero_grad()
                global_step += 1

                if config.local_rank in [-1, 0] and config.logging_steps > 0 and global_step % config.logging_steps == 0:
                    # Log metrics
                    # Only evaluate when single GPU otherwise metrics may not average well
                    if config.local_rank == -1 and config.evaluate_during_training:
                        results = evaluate(config, model, tokenizer)
                    logging_loss = tr_loss
                if config.local_rank in [-1, 0] and config.save_steps > 0 and global_step % config.save_steps == 0:
                    # Save model checkpoint
                    output_dir = os.path.join(
                        config.output_dir, 'checkpoint-{}'.format(global_step))
                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir)
                    # Take care of distributed/parallel training
                    model_to_save = model.module if hasattr(
                        model, 'module') else model
                    model_to_save.save_pretrained(output_dir)
                    torch.save(config, os.path.join(
                        output_dir, 'training_config.bin'))
                    logger.info("Saving model checkpoint to %s", output_dir)

            if 0 < config.max_steps < global_step:
                epoch_iterator.close()
                break
        if 0 < config.max_steps < global_step:
            train_iterator.close()
            break
    return global_step, tr_loss / global_step


def evaluate(config, model, tokenizer, prefix=""):
    # Loop to handle MNLI double evaluation (matched, mis-matched)
    eval_task = config.task_name
    eval_output_dir = config.output_dir

    results = {}
    eval_dataset = load_and_cache_examples(
        config, eval_task, tokenizer, evaluate=True)

    if not os.path.exists(eval_output_dir) and config.local_rank in [-1, 0]:
        os.makedirs(eval_output_dir)

    config.eval_batch_size = config.per_gpu_eval_batch_size * \
        max(1, config.n_gpu)
    # Note that DistributedSampler samples randomly
    eval_sampler = SequentialSampler(
        eval_dataset) if config.local_rank == -1 else DistributedSampler(eval_dataset)
    eval_dataloader = DataLoader(
        eval_dataset, sampler=eval_sampler, batch_size=config.eval_batch_size, shuffle=False)

    # Eval!
    logger.info("***** Running evaluation {} *****".format(prefix))
    logger.info("  Num examples = %d", len(eval_dataset))
    logger.info("  Batch size = %d", config.eval_batch_size)
    eval_loss = 0.0
    nb_eval_steps = 0
    preds = None
    out_label_ids = None
    for batch in tqdm(eval_dataloader, desc="Evaluating"):
        model.eval()
        batch = tuple(t.to(config.device) for t in batch)

        with torch.no_grad():
            inputs = {'input_ids':      batch[0],
                      'attention_mask': batch[1],
                      # XLM and RoBERTa don't use segment_ids
                      'token_type_ids': batch[2],
                      'labels':      batch[3],
                      'e1_mask': batch[4],
                      'e2_mask': batch[5],
                      }
            outputs = model(**inputs)
            tmp_eval_loss, logits = outputs[:2]

            eval_loss += tmp_eval_loss.mean().item()
        nb_eval_steps += 1
        if preds is None:
            preds = logits.detach().cpu().numpy()
            out_label_ids = inputs['labels'].detach().cpu().numpy()
        else:
            preds = np.append(preds, logits.detach().cpu().numpy(), axis=0)
            out_label_ids = np.append(
                out_label_ids, inputs['labels'].detach().cpu().numpy(), axis=0)

    eval_loss = eval_loss / nb_eval_steps
    print('eval_loss: {}'.format(eval_loss))
    preds = np.argmax(preds, axis=1)
    result = compute_metrics(eval_task, preds, out_label_ids)
    print(result)
    results.update(result)
    logger.info("***** Eval results {} *****".format(prefix))
    for key in sorted(result.keys()):
        logger.info(f"{key} = {result[key]}")
    
    if config.task_name == "semeval":
        output_eval_file = "eval/sem_res.txt"
        with open(output_eval_file, "w") as writer:
            for key in range(len(preds)):
                writer.write("%d\t%s\n" % (key, str(preds[key])))
    return result


def load_and_cache_examples(config, task, tokenizer, evaluate=False, test=False):

    processor = data_processors[config.task_name]()
    output_mode = "classification"

    # Load data features from cache or dataset file
    evaluation_set_name = 'test' if test else 'dev'  # 评估集为test or dev
    cached_features_file = os.path.join(config.data_dir,
                                        'cached_{}_{}_{}_{}'.format(evaluation_set_name if evaluate else 'train',
                                        list(filter(None, config.pretrained_model_name.split('/'))).pop(),
                                        str(config.max_seq_len),  # 128
                                        str(task)))  # task_name=semeval
    # print(cached_features_file)  # ./data/cached_train_bert-base-cased_128_semeval

    if os.path.exists(cached_features_file):
        logger.info(f"Loading features from cached file {cached_features_file}")
        features = torch.load(cached_features_file)
    else:  # 如果cache不存在
        logger.info("Creating features from dataset file at %s",
                    config.data_dir)
        label_list = processor.get_labels()  # ['0', '1', ..., '18']
        examples = processor.get_dev_examples(config.data_dir) if evaluate else processor.get_train_examples(config.data_dir)  # utils.InputExample对象列表
        # print(examples[0].text_a)  # 'the system as described above has its greatest application in an arrayed [E11] configuration [E12] of antenna [E21] elements [E22]'
        # print(examples[0].label)  # '12'
        # 将句子转换成符合BERT输入规范的内容
        features = convert_examples_to_features(examples, label_list, config.max_seq_len, tokenizer, "classification", use_entity_indicator=config.use_entity_indicator)

        logger.info(f"Saving features into cached file {cached_features_file}")
        torch.save(features, cached_features_file)

    if config.local_rank == 0 and not evaluate:
        # Make sure only the first process in distributed training process the dataset, and the others will use the cache
        torch.distributed.barrier()
    # Convert to Tensors and build dataset
    all_input_ids = torch.tensor(
        [f.input_ids for f in features], dtype=torch.long)
    all_input_mask = torch.tensor(
        [f.input_mask for f in features], dtype=torch.long)
    all_segment_ids = torch.tensor(
        [f.segment_ids for f in features], dtype=torch.long)
    all_e1_mask = torch.tensor(
        [f.e1_mask for f in features], dtype=torch.long)  # add e1 mask
    all_e2_mask = torch.tensor(
        [f.e2_mask for f in features], dtype=torch.long)  # add e2 mask
    if output_mode == "classification":
        all_label_ids = torch.tensor(
            [f.label_id for f in features], dtype=torch.long)
    elif output_mode == "regression":
        all_label_ids = torch.tensor(
            [f.label_id for f in features], dtype=torch.float)
    dataset = TensorDataset(all_input_ids, all_input_mask,
                            all_segment_ids, all_label_ids, all_e1_mask, all_e2_mask)
    return dataset


def main():
    do_lower_case = False

    parser = ArgumentParser(description="BERT for relation extraction (classification)")
    parser.add_argument('--config', dest='config')
    args = parser.parse_args()  # 将parser对象使用"add_argument"添加的参数返回给args对象，args对象可以直接使用这些参数
    config = Config(args.config)

    if os.path.exists(config.output_dir) and os.listdir(config.output_dir) and config.train and not config.overwrite_output_dir:
        raise ValueError("Output directory ({}) already exists and is not empty. "
                         "Use --overwrite_output_dir to overcome.".format(config.output_dir))

    # Setup CUDA, GPU & distributed training
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    config.n_gpu = 1
    config.device = device

    # Setup logging 日志
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S',
                        level=logging.INFO if config.local_rank in [-1, 0] else logging.WARN)
    logger.warning("Process rank: %s, device: %s, n_gpu: %s, distributed training: %s",
                   config.local_rank, device, config.n_gpu, bool(config.local_rank != -1))

    # Set seed
    set_seed(config.seed)

    # Prepare task -- SemEval
    processor = data_processors[config.task_name]()  # <class 'utils.SemEvalProcessor'>
    label_list = processor.get_labels()  # [0, 1, ..., 18]
    num_labels = len(label_list)  # 19 for SemEval
    # train_example = processor.get_train_examples(config.data_dir)  # utils.InputExample对象列表

    # Load pretrained model and tokenizer
    bertconfig = BertConfig.from_pretrained('bert-base-cased', num_labels=num_labels, finetuning_task=config.task_name)
    tokenizer = BertTokenizer.from_pretrained(
        'bert-base-cased', do_lower_case=do_lower_case, additional_special_tokens=additional_special_tokens_2)
    # additional_special_tokens = ["[E11]", "[E12]", "[E21]", "[E22]"]
    model = BertForSequenceClassification.from_pretrained('bert-base-cased', config=bertconfig)
    model.resize_token_embeddings(len(tokenizer))
    model.to(config.device)

    # Training
    if config.train:
        train_dataset = load_and_cache_examples(config, config.task_name, tokenizer, evaluate=False)
        # print(train_dataset)  # torch

        global_step, tr_loss = train(config, train_dataset, model, tokenizer)  # 训练
        logger.info(" global_step = %s, average loss = %s", global_step, tr_loss)

    # Saving best-practices: if you use defaults names for the model, you can reload it using from_pretrained()
    if config.train and (config.local_rank == -1 or torch.distributed.get_rank() == 0):
        # Create output directory if needed
        if not os.path.exists(config.output_dir) and config.local_rank in [-1, 0]:
            os.makedirs(config.output_dir)

        logger.info("Saving model checkpoint to %s", config.output_dir)
        # Save a trained model, configuration and tokenizer using `save_pretrained()`.
        # They can then be reloaded using `from_pretrained()`
        # Take care of distributed/parallel training
        model_to_save = model.module if hasattr(model, 'module') else model
        model_to_save.save_pretrained(config.output_dir)
        tokenizer.save_pretrained(config.output_dir)

        # Good practice: save your training arguments together with the trained model
        torch.save(config, os.path.join(config.output_dir, 'training_config.bin'))

        # Load a trained model and vocabulary that you have fine-tuned
        model = BertForSequenceClassification.from_pretrained(config.output_dir)
        tokenizer = BertTokenizer.from_pretrained(
            config.output_dir, do_lower_case=do_lower_case, additional_special_tokens=additional_special_tokens_2)
        model.to(config.device)

    # Evaluation
    results = {}
    if config.eval and config.local_rank in [-1, 0]:
        tokenizer = BertTokenizer.from_pretrained(
            config.output_dir, do_lower_case=do_lower_case, additional_special_tokens=additional_special_tokens_2)
        checkpoints = [config.output_dir]
        if config.eval_all_checkpoints:  # 评估所有检查点
            checkpoints = list(os.path.dirname(c) for c in sorted(
                glob.glob(config.output_dir + '/**/' + WEIGHTS_NAME, recursive=True)))
            logging.getLogger("pytorch_transformers.modeling_utils").setLevel(
                logging.WARN)  # Reduce logging
        logger.info("Evaluate the following checkpoints: %s", checkpoints)
        for checkpoint in checkpoints:
            global_step = checkpoint.split(
                '-')[-1] if len(checkpoints) > 1 else ""
            model = BertForSequenceClassification.from_pretrained(checkpoint)
            model.to(config.device)
            result = evaluate(config, model, tokenizer, prefix=global_step)
            result = dict((k + '_{}'.format(global_step), v)
                          for k, v in result.items())
            results.update(result)

    return results


if __name__ == "__main__":
    main()
