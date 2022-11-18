
import json
import torch
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


if __name__ == "__main__":
    # one plot, single layer, 12 epoch samples
    prefix = r'./results/result-4/'
    output_folder = 'layer-plot-2/'
    layer = '3'
    vmin = 0.6
    vmax = 1.0
    cmap = 'YlGnBu'
    # cmap = 'RdYlBu', 'YlOrRd', 'YlGnBu'

    epochs = ['epoch_0', 'epoch_2', 'epoch_4', 'epoch_6', 'epoch_9', 'epoch_12',
              'epoch_15', 'epoch_18', 'epoch_21', 'epoch_24', 'epoch_27', 'epoch_29']
    fig = plt.figure(figsize=(32, 24))
    for i, epoch in enumerate(epochs):
        file = prefix + epoch + '_layer_' + layer + '.txt'
        with open(file, 'r', encoding='utf-8') as f:
            content = f.readlines()
            vector_list = [json.loads(vec) for vec in content]

            vec_tensor_t = torch.tensor(vector_list).t()  # tensor(4096, 80)
            vec_df = pd.DataFrame(vec_tensor_t)
            vec_corr = vec_df.corr(method='spearman')  # 默认是pearson相关系数

            fig.add_subplot(3, 4, i+1)
            mask = np.eye(vec_tensor_t.size(1))  # 对角线元素为1
            # 设置坐标轴字体大小
            plt.xticks(fontsize=21)
            plt.yticks(fontsize=21)
            # 获取图像句柄，调整color bar字体大小
            ax = sns.heatmap(vec_corr, square=True, mask=mask, vmin=vmin, vmax=vmax,
                             cmap=cmap, xticklabels=20, yticklabels=20, cbar=False)
            cbar = ax.figure.colorbar(ax.collections[0])
            cbar.ax.tick_params(labelsize=22)
            # 设置图像标题
            title = epoch + '_layer_' + layer
            plt.title(title, fontdict={'size': 23})

            print(title + ' --- done')
    plt.savefig(prefix + output_folder + 'layer_' + layer + '.png')
