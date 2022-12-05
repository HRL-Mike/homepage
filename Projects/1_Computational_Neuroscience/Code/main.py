
import csv
import json
import torch
import torch.nn as nn

from torch.utils.data import DataLoader
from vgg16_model_2 import VGGnet
from data_loader import DataGenerater

from torchvision.transforms import transforms
from PIL import Image


class FeatureExtractor(nn.Module):
    def __init__(self, model):
        super(FeatureExtractor, self).__init__()
        # Extract VGG-16 Feature Layers
        self.features = model.features
        set_parameter_requires_grad(self.features)  # 固定特征提取层参数
        # Extract VGG-16 Average Pooling Layer
        self.avgpool = model.avgpool
        # Extract the first part of fully-connected layer from VGG16
        self.classifier = model.classifier

    def get_conv3_maxpool_output(self, x):
        out = self.features[:17](x)  # torch.Size([1, 256, 28, 28]), 200,704维
        out = torch.flatten(out, 1)
        return out

    def get_conv4_maxpool_output(self, x):
        out = self.features[:24](x)  # torch.Size([1, 512, 14, 14]), 100,352维
        out = torch.flatten(out, 1)
        return out

    def get_conv5_maxpool_output(self, x):
        out = self.features(x)  # torch.Size([1, 512, 7, 7]), 25,088维
        out = self.avgpool(out)  # torch.Size([1, 512, 7, 7])
        out = torch.flatten(out, 1)
        return out

    def get_fc1_output(self, x):
        out = self.features(x)
        out = self.avgpool(out)
        out = torch.flatten(out, 1)
        out = self.classifier[:2](out)  # torch.Size([1, 4096])
        return out

    def get_fc2_output(self, x):
        out = self.features(x)
        out = self.avgpool(out)
        out = torch.flatten(out, 1)
        out = self.classifier[:5](out)  # torch.Size([1, 4096])
        return out

    def get_fc3_output(self, x):
        out = self.features(x)
        out = self.avgpool(out)
        out = torch.flatten(out, 1)
        out = self.classifier(out)  # torch.Size([1, 4])
        return out


def set_parameter_requires_grad(model):
    for param in model.parameters():
        param.requires_grad = False


def calculate_feature_vector(model, image_list, device):
    conv3_list = []
    conv4_list = []
    conv5_list = []
    fc1_list = []
    fc2_list = []
    fc3_list = []

    extractor = FeatureExtractor(model)
    extractor = extractor.to(device)
    tf = transforms.Compose([  # 常用的数据变换器
        lambda x: Image.open(x).convert('RGB'),  # string path => image data
        # transforms.RandomHorizontalFlip(p=0.3),
        # transforms.RandomRotation(45),
        # transforms.Grayscale(3),  # 3通道灰度图像
        transforms.ToTensor()
        # transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])  # 加速收敛, 不归一化也可以
    ])
    with torch.no_grad():
        for image in image_list:
            img = tf(image)  # [3, 224, 224]
            img = img.unsqueeze(0)  # to [1, 3, 224, 224]
            img = img.to(device)
            conv3_features = extractor.get_conv3_maxpool_output(img)
            conv3_list.append(conv3_features)
            conv4_features = extractor.get_conv4_maxpool_output(img)
            conv4_list.append(conv4_features)
            conv5_features = extractor.get_conv5_maxpool_output(img)
            conv5_list.append(conv5_features)
            fc1_features = extractor.get_fc1_output(img)
            fc1_list.append(fc1_features)
            fc2_features = extractor.get_fc2_output(img)
            fc2_list.append(fc2_features)
            fc3_features = extractor.get_fc3_output(img)
            fc3_list.append(fc3_features)
    conv3_list = [vec.tolist()[0] for vec in conv3_list]
    conv4_list = [vec.tolist()[0] for vec in conv4_list]
    conv5_list = [vec.tolist()[0] for vec in conv5_list]
    fc1_list = [vec.tolist()[0] for vec in fc1_list]
    fc2_list = [vec.tolist()[0] for vec in fc2_list]
    fc3_list = [vec.tolist()[0] for vec in fc3_list]
    return conv3_list, conv4_list, conv5_list, fc1_list, fc2_list, fc3_list
    # python函数返回多个值时，默认以tuple形式返回。如果赋值给一个变量，将会把整个元组赋值给变量。


def get_images(folder_path):
    image_dict = {'car': [], 'fruit': [], 'human_face': [], 'monkey_face': []}
    with open(folder_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            image_path = row[0]
            label = row[1]
            if label == '0':
                image_dict['car'].append(image_path)
            elif label == '1':
                image_dict['fruit'].append(image_path)
            elif label == '2':
                image_dict['human_face'].append(image_path)
            elif label == '3':
                image_dict['monkey_face'].append(image_path)
    return image_dict


if __name__ == "__main__":
    # 参数
    learning_rate = 0.00002  # 1e-6 --> 2e-5
    num_epochs = 30
    batch_size = 16
    prefix = r'./results/result-4/'

    # 加载数据集
    train_dataset = DataGenerater('resized_dataset', 224, 'train')  # root, resize, mode
    val_dataset = DataGenerater('resized_dataset', 224, 'val')
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)  # 已经是tensor了
    test_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=True)

    # 初始化
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = VGGnet().to(device)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

    # 准备测试数据
    test_set_path = r'./resized_dataset/test_images_3.csv'  # 4*20=80张
    test_image_dict = get_images(test_set_path)

    epoch_dict = {}
    for num in range(num_epochs):
        k = 'epoch_' + str(num)
        epoch_dict[k] = None

    # 训练
    acc_list = []
    total_step = len(train_loader)  # 66
    for epoch in range(num_epochs):
        model.train()
        for i, (images, labels) in enumerate(train_loader):
            images = images.to(device)  # mini-batch
            labels = labels.to(device)
            # Forward
            outputs = model(images)
            loss = criterion(outputs, labels)
            # Backward and optimize
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            # 进度
            if (i + 1) % 2 == 0:
                print('Epoch [{}/{}], Step [{}/{}], Loss: {:.4f}'
                      .format(epoch + 1, num_epochs, i + 1, total_step, loss.item()))

        # 训练完一轮后, 评估模型并计算feature vector
        model.eval()
        with torch.no_grad():
            # 先评估一下模型精度
            correct = 0
            total = 0
            for images, labels in test_loader:  # 使用测试数据进行评估
                images = images.to(device)
                labels = labels.to(device)
                outputs = model(images)
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
            acc = 100 * correct / total
            print('Test Accuracy: {:.8f} %'.format(acc))
            acc_list.append(acc)

            # 计算图像的特征向量
            # 返回conv3_list, conv4_list, conv5_list, fc1_list, fc2_list
            car_conv3_list, car_conv4_list, car_conv5_list, car_fc1_list, car_fc2_list, car_fc3_list \
                = calculate_feature_vector(model, test_image_dict['car'], device)
            fruit_conv3_list, fruit_conv4_list, fruit_conv5_list, fruit_fc1_list, fruit_fc2_list, fruit_fc3_list \
                = calculate_feature_vector(model, test_image_dict['fruit'], device)
            human_conv3_list, human_conv4_list, human_conv5_list, human_fc1_list, human_fc2_list, human_fc3_list \
                = calculate_feature_vector(model, test_image_dict['human_face'], device)
            monkey_conv3_list, monkey_conv4_list, monkey_conv5_list, monkey_fc1_list, monkey_fc2_list, monkey_fc3_list \
                = calculate_feature_vector(model, test_image_dict['monkey_face'], device)

            # 写入文档
            epoch_num = 'epoch_' + str(epoch)
            conv3_vector_list = car_conv3_list + fruit_conv3_list + human_conv3_list + monkey_conv3_list
            file = prefix + epoch_num + '_layer_3.txt'
            with open(file, 'w', encoding='utf-8') as f:
                for vec in conv3_vector_list:
                    f.write(json.dumps(vec) + '\n')
            conv4_vector_list = car_conv4_list + fruit_conv4_list + human_conv4_list + monkey_conv4_list
            file = prefix + epoch_num + '_layer_4.txt'
            with open(file, 'w', encoding='utf-8') as f:
                for vec in conv4_vector_list:
                    f.write(json.dumps(vec) + '\n')
            conv5_vector_list = car_conv5_list + fruit_conv5_list + human_conv5_list + monkey_conv5_list
            file = prefix + epoch_num + '_layer_5.txt'
            with open(file, 'w', encoding='utf-8') as f:
                for vec in conv5_vector_list:
                    f.write(json.dumps(vec) + '\n')
            fc1_vector_list = car_fc1_list + fruit_fc1_list + human_fc1_list + monkey_fc1_list
            file = prefix + epoch_num + '_layer_6.txt'
            with open(file, 'w', encoding='utf-8') as f:
                for vec in fc1_vector_list:
                    f.write(json.dumps(vec) + '\n')
            fc2_vector_list = car_fc2_list + fruit_fc2_list + human_fc2_list + monkey_fc2_list
            file = prefix + epoch_num + '_layer_7.txt'
            with open(file, 'w', encoding='utf-8') as f:
                for vec in fc2_vector_list:
                    f.write(json.dumps(vec) + '\n')
            fc3_vector_list = car_fc3_list + fruit_fc3_list + human_fc3_list + monkey_fc3_list
            file = prefix + epoch_num + '_layer_8.txt'
            with open(file, 'w', encoding='utf-8') as f:
                for vec in fc3_vector_list:
                    f.write(json.dumps(vec) + '\n')

    file = prefix + 'acc_list.txt'
    with open(file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(acc_list))

    # 更改学习率，看看模型精度  Done
    # 加入新图片 Done
    # pretrain = False
    # 灰度图像
    # 80*80的图，下三角要减去均值？
    # 取均值
