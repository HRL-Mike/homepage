
import torch.nn as nn
from torchvision import models


class AlexNet(nn.Module):
    def __init__(self, feature_extract=True, num_classes=4):
        super(AlexNet, self).__init__()
        # 导入VGG16模型
        model = models.alexnet(pretrained=True)  # 使用ImageNet数据集预训练
        # 加载features部分
        self.features = model.features
        # 固定特征提取层参数
        set_parameter_requires_grad(self.features, feature_extract)
        # 加载avgpool层
        self.avgpool = model.avgpool
        # 改变classifier：分类层
        fcl = model.classifier
        fcl[6] = nn.Linear(4096, num_classes)
        self.classifier = fcl

    def forward(self, x):
        x = self.features(x)
        x = self.avgpool(x)  # torch.Size([16, 256, 6, 6])
        x = x.view(x.size(0), 256 * 6 * 6)
        out = self.classifier(x)
        return out
    # torch.Size([16, 512, 7, 7])
    # torch.Size([16, 25088])


# 固定参数，不进行训练
def set_parameter_requires_grad(model, feature_extracting):
    if feature_extracting:
        for param in model.parameters():
            param.requires_grad = False


if __name__ == "__main__":
    # 原始模型
    model = models.alexnet()
    print(model.features)
    print(model.avgpool)
    print(model.classifier)

    # # 修改后的模型
    # new_vgg11 = AlexNet()
    # print(new_vgg11.features)
    # print(new_vgg11.avgpool)
    # print(new_vgg11.classifier)
