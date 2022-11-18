import os
import torch
import glob
import csv
import random

from torch.utils.data import Dataset, DataLoader  # 自定义的父类
from torchvision.transforms import transforms
from PIL import Image


class DataGenerater(Dataset):
    def __init__(self, root, resize, mode):
        super(DataGenerater, self).__init__()
        self.root = root
        self.resize = resize
        self.name2label = {}  # "sq...":0
        for name in sorted(os.listdir(os.path.join(root))):
            if not os.path.isdir(os.path.join(root, name)):
                continue
            self.name2label[name] = len(self.name2label.keys())  # 将英文标签名转化数字0-4
        # print(self.name2label)  # {'car': 0, 'fruit': 1, 'human_face': 2, 'monkey_face': 3}

        # image, label
        self.images, self.labels = self.load_csv('images.csv')  # 返回图片路径列表和标签列表
        if mode == 'train':  # 80%
            self.images = self.images[:int(0.9 * len(self.images))]
            self.labels = self.labels[:int(0.9 * len(self.labels))]
        elif mode == 'val':  # 20% = 80%:100%
            self.images = self.images[int(0.9 * len(self.images)):]
            self.labels = self.labels[int(0.9 * len(self.labels)):]
            # self.write_csv('test_images.csv')

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        # idx~[0~len(images)]
        # self.images, self.labels
        # img: 'resized_image\\car\\car-39.png'
        # label: 0
        img, label = self.images[idx], self.labels[idx]

        tf = transforms.Compose([  # 常用的数据变换器
            lambda x: Image.open(x).convert('RGB'),  # string path => image data
            # 这里开始读取了数据的内容了
            # transforms.Resize(  # 数据预处理部分
            #     (int(self.resize * 1.25), int(self.resize * 1.25))),
            transforms.RandomRotation(15),
            transforms.CenterCrop(self.resize),  # 防止旋转后边界出现黑框部分
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225])
        ])
        img = tf(img)
        label = torch.tensor(label)  # 转化tensor
        return img, label  # 返回当前的数据内容和标签

    def load_csv(self, filename):
        # 如果没有写好的csv文件，则创建一个包含图像路径和标签的csv文件
        if not os.path.exists(os.path.join(self.root, filename)):
            images = []
            for name in self.name2label.keys():
                images += glob.glob(os.path.join(self.root, name, '*.png'))
                images += glob.glob(os.path.join(self.root, name, '*.jpg'))
                images += glob.glob(os.path.join(self.root, name, '*.jpeg'))
                # 'resized_image\\car\\car-39.png'

            random.seed(128)
            random.shuffle(images)
            with open(os.path.join(self.root, filename), mode='w', newline='') as f:
                writer = csv.writer(f)
                for img in images:  # len = 1302
                    pic_type = img.split(os.sep)[-2]  # monkey_face
                    label = self.name2label[pic_type]
                    # 'resized_image\\car\\car-39.png', 0
                    writer.writerow([img, label])  # 写入csv文件

        # 读取csv文件
        images, labels = [], []
        with open(os.path.join(self.root, filename)) as f:
            reader = csv.reader(f)
            for row in reader:
                img, label = row  # ['resized_image\\human_face\\human_face-227.png', '2']
                label = int(label)
                images.append(img)
                labels.append(label)
        assert len(images) == len(labels)
        return images, labels

    # def write_csv(self, filename):
    #     with open(os.path.join(self.root, filename), mode='w', newline='') as f:
    #         writer = csv.writer(f)
    #         for img in self.images:
    #             pic_type = img.split(os.sep)[-2]  # monkey_face
    #             label = self.name2label[pic_type]
    #             writer.writerow([img, label])  # 写入csv文件

    def denormalize(self, x_hat):
        mean = [0.485, 0.456, 0.406]
        std = [0.229, 0.224, 0.225]
        # x_hat = (x-mean)/std
        # x = x_hat*std = mean
        # x: [c, h, w]
        # mean: [3] => [3, 1, 1]
        mean = torch.tensor(mean).unsqueeze(1).unsqueeze(1)
        std = torch.tensor(std).unsqueeze(1).unsqueeze(1)
        # print(mean.shape, std.shape)
        x = x_hat * std + mean
        return x


if __name__ == '__main__':
    db = DataGenerater('resized_dataset', 224, 'val')
    loader = DataLoader(db, batch_size=16, shuffle=True)
    # for x, y in loader:  # 此时x,y是批量的数据
    #     print(x.shape)
