#! /usr/bin/env python
# -*- encoding: utf-8 -*-
import numpy as np
import matplotlib.pylab as plt
from mpl_toolkits.mplot3d import Axes3D

#定义一个简单的树结构
class RTree:
    def __init__(self,data,z,slicedIdx):
        self.data =data
        self.z =z
        self.isLeaf = True
        self.slicedIdx = slicedIdx #节点上只保存数据的序号,不保存数据子集,节约内存
        self.left =None
        self.right = None
        self.output = np.mean(z[slicedIdx])
        self.j = None
        self.s = None
    #本节点所带的子数据如果大于1个,则生成两个叶子节点,本节点不再是叶子节点
    def grow(self):
        if len(self.slicedIdx)>1:
            j,s,_ = bestDivi(self.data,self.z,self.slicedIdx)
            leftIdx, rightIdx = [], []
            for i in self.slicedIdx:
                if self.data[i,j]<s:
                    leftIdx.append(i)
                else:
                    rightIdx.append(i)
            self.isLeaf =False
            self.left = RTree(self.data,self.z,leftIdx)
            self.right = RTree(self.data,self.z,rightIdx)
            self.j=j
            self.s=s
    def err(self):
        return np.mean((self.z[self.slicedIdx]-self.output)**2)

#计算平方差
def squaErr(data,output,slicedIdx,j,s):
    #挑选数据子集
    region1 = []
    region2 = []
    for i in slicedIdx:
        if data[i,j]<s:
            region1.append(i)
        else:
            region2.append(i)
    # print region1,'111'
    # print region2
    #计算子集上的平均输出
    c1 = np.mean(output[region1])
    err1 = np.sum((output[region1]-c1)**2)

    c2 = np.mean(output[region2])
    err2 = np.sum((output[region2]-c2)**2)
    #返回平方差
    return err1+err2

#用于选择最佳划分属性j和最切分点s
def bestDivi(data,z,slicedIdx):
    min_j = 0
    sortedValue = np.sort(data[slicedIdx][:,min_j])
    min_s = (sortedValue[0]+sortedValue[1])/2
    # print min_s
    err = squaErr(data,z,slicedIdx,min_j,min_s)
    #遍历属性
    for j in range(data.shape[1]):
        #产生某个属性值的分割点集合
        sortedValue = np.sort(data[slicedIdx][:,j])
        sliceValue = (sortedValue[1:]+sortedValue[:-1])/2
        # print sliceValue
        for s in sliceValue:
            errNew = squaErr(data,z,slicedIdx,j,s)
            if errNew < err:
                err = errNew
                min_j = j
                min_s = s

    return min_j, min_s, err

#更新树
def updateTree(tree):
    if tree.isLeaf:
        tree.grow()
    else:
        updateTree(tree.left)
        updateTree(tree.right)

#预测一个数据点的输出
def predict(single_data,init_tree):
    tree = init_tree
    while True:
        if tree.isLeaf:
            return tree.output
        else:
            if single_data[tree.j] < tree.s:
                tree = tree.left
            else:
                tree = tree.right

#利用z=x+y+noise 人为生成一个数据集, 具有2个特征
n_samples = 300
points = np.random.rand(n_samples,2)
# print points
z = points[:,0]+points[:,1] + 0.2*(np.random.rand(n_samples)-0.5)
#生成根节点
# print range(n_samples)
root = RTree(points,z,range(n_samples))
# print points.shape[1]
#进行五次生长, 观测每次生长过后的拟合效果
for ii in range(5):
    updateTree(root)
    z_predicted = np.array([predict(p,root) for p in points])
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(111,projection="3d")
    ax.scatter(points[:,0],points[:,1],z)
    ax.scatter(points[:,0],points[:,1],z_predicted)
    plt.show()

