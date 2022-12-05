
import torch.nn as nn
from torchvision import models


class VGGnet(nn.Module):
    def __init__(self, feature_extract=True, num_classes=4):
        super(VGGnet, self).__init__()
        # 导入VGG16模型
        model = models.vgg16(pretrained=True)
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
        x = self.avgpool(x)
        x = x.view(x.size(0), 512 * 7 * 7)
        out = self.classifier(x)
        return out


# 固定参数，不进行训练
def set_parameter_requires_grad(model, feature_extracting):
    if feature_extracting:
        for param in model.parameters():
            param.requires_grad = False


if __name__ == "__main__":
    # 原始模型
    # vgg16 = models.vgg16()
    # print(vgg16)
    #
    # # 修改后的模型
    new_vgg16 = VGGnet()
    print('model_build', '**'*20)
    print(new_vgg16)
