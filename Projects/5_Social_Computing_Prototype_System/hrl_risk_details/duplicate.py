# -*- coding: utf-8 -*-
# User: linhaobuaa
# Date: 2014-12-28 17:00:00
# Version: 0.3.0

import json
import sys

reload(sys)
sys.setdefaultencoding( "utf-8" )

def duplicate(items):
    """
    批量文本去重, 输入的文本可以有部分已经去完重的，以duplicate字段标识
       input:
           items: 一推文本，[{"_id": , "title": , "content": }], 
           文本以utf-8编码
       output:
           更新了duplicate和same_from字段的items， same_from链向相似的新闻的_id
    """
    not_same_items = [item for item in items if 'duplicate' in item and item['duplicate'] == False]
    duplicate_items = [item for item in items if 'duplicate' in item and item['duplicate'] == True]
    candidate_items = [item for item in items if 'duplicate' not in item]

    for item in candidate_items:
        idx, rate, flag = max_same_rate_shingle(not_same_items, item)
        if flag:
            item['duplicate'] = False
            item['same_from'] = item['_id']
            not_same_items.append(item)
        else:
            item['duplicate'] = True
            item['same_from'] = not_same_items[idx]['_id']
            duplicate_items.append(item)

    return not_same_items + duplicate_items


class ShingLing(object):
    """shingle算法
    """
    def __init__(self, text1, text2, n=3):
        """input
               text1: 输入文本1, unicode编码
               text2: 输入文本2, unicode编码
               n: 切片长度
        """
        if not isinstance(text1, unicode):
            raise ValueError("text1 must be unicode")

        if not isinstance(text2, unicode):
            raise ValueError("text2 must be unicode")

        self.n = n
        self.threshold = 0.2
        self.text1 = text1
        self.text2 = text2
        self.set1 = set()
        self.set2 = set()
        self._split(self.text1, self.set1)
        self._split(self.text2, self.set2)
        self.jaccard = 0

    def _split(self, text, s):
        if len(self.text1) < self.n:
            self.n = 1

        for i in range(len(text) - self.n + 1):
            piece = text[i: i + self.n]
            s.add(piece)

    def cal_jaccard(self):
        intersection_count = len(self.set1 & self.set2)
        union_count = len(self.set1 | self.set2)

        if union_count == 0:
            self.jaccard = 0.0
            return self.jaccard

        self.jaccard = float(intersection_count) / float(union_count)
        return self.jaccard

    def check_duplicate(self):
        return True if self.jaccard > self.threshold else False

def max_same_rate_shingle(items, item, rate_threshold = 0.3):
    """input:
           items: 已有的不重复数据
           item: 待检测的数据
       output:
           idx: 相似的下标
           max_rate: 相似度
           flag: True表示不相似
    """
    flag = True
    idx = 0
    max_rate = 0
    for i in items:
        sl = ShingLing((i['title']).decode('utf-8'), (item['title']).decode('utf-8'), n=3)
        sl.cal_jaccard()
        if sl.jaccard >= rate_threshold:
            max_rate = sl.jaccard
            flag = False
            break

        idx += 1

    if flag == True:
        idx = 0
        max_rate = 0
        for i in items:
            sl = ShingLing((i['title'] + i['content']).decode('utf-8'), (item['title'] + item['content']).decode('utf-8'), n=3)
            sl.cal_jaccard()
            if sl.jaccard >= rate_threshold:
                max_rate = sl.jaccard
                flag = False
                break
            idx += 1

    return idx, max_rate, flag


if __name__ == '__main__':
    text1 = u"中国中央电视台"
    text2 = u"中央电视台广播"
    s = ShingLing(text1, text2, 3)
    print s.cal_jaccard()
    print s.check_duplicate()


    test = [{"content": "打蜡几乎是必然的工序,更有甚者,一些商家用一种含有“抑霉唑”农药成分的杀菌剂,替代食品级果蜡给花牛...", "news_date": "2018-01-01", "media": "网易", "_id": 0, "title": "商家将国产苹果抹微毒农药伪装成美国品牌出售"}, {"content": "中国农药网2015/8/2716:24:28来源:本网论坛【大中小】为进一步提高市植保站检疫人员对外来生物入侵的认识,更加全面准确的掌握这门学科的理论与方法体系...", "news_date": "2018-01-01", "_id": 1, "title": "北京市植保站参加首届“入侵生物学理论与方法高级研修班”"}, {"content": "此任务在芳邻镇触发。前往芳邻镇和商店主黛西交谈并选择“找工作”,她会说超级变种人已经占领了波士顿图书馆...", "news_date": "2018-01-01", "_id": 2, "title": "《辐射4》支线任务攻略辐射4支线任务攻略大全"}, {"content": "脑卒中是可防可控的,脑卒中发生的危险因素,可分为可防治与不可防治的,老化与遗传基因就是其中不可防治的...", "_id": 3, "title": "为何心跳快的人更易猝死|脑卒中|危险_凤凰健康"}, {"content": "钾可激活酶的活性,促进光合作用,加快淀粉和糖类的运转,增强玉米的抗旱抗逆能力,防止病虫害侵入,提高水分利用率,预防倒伏,延长贮存期,提高产量和品质。玉米缺钾症...", "_id": 4, "title": "玉米缺钾的原因和防治方法"}, {"content": "受公安部科技信息化局委托,我所在天津主持召开了公安部应用创新计划项目《车载LNG泄漏火灾爆炸事故防治技术研究...", "media": "网易", "_id": 5, "title": "《车载LNG泄漏火灾爆炸事故防治技术研究》获验收"}, {"content": "联合国环境规划署、联合国防治荒漠化公约组织等机构认为,中国是全球沙漠治理的典范,内蒙古是中国沙漠治理的先锋,库布其是中国沙漠生态治理的缩影。亿利资源集团探索出...", "media": "网易", "_id": 6, "title": "联合国多家机构:中国是全球沙漠治理的典范"}, {"content": "泉州的水污染防治,再上一个新高度。明年起,泉州将每季度向社会定期公布饮用水的水质状况。昨日,海都记者从泉州市环保局获悉,泉州市政府印发了《泉州市水污染防治...",  "media": "网易", "_id": 7, "title": "泉州明年起定期公开饮用水源水质"}]
    print json.dumps(duplicate(test), encoding='UTF-8', ensure_ascii=False)
