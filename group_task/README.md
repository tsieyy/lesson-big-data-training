# 团体任务

## python连接mysql数据库和CRUD

内容在`_01pymysql.py`，用pymysql连数据库进行CRUD的操作。

我这里直接和第二步连一起，用scrapy爬完后直接存到mysql里面。


## python编写爬虫

爬虫不会，代码搬运github的一个爬取赛氪网的一个项目。

运行 `_02main.py`，使用scrapy爬取后把数据写入mysql中。

## 大数据运维（配置安装hadoop及其组件）

该部分使用docker进行搭建，使用ubuntu镜像自己安装了文档中所需要的组件：spark、zookeeper等等。

统一安装在`/usr/local`目录下，配置只配置了hadoop一项和基本的java环境，其他的都没怎么动，需要的话就自己配置一下。

**获取docker镜像步骤：**
 
1. 输入`docker search tsieyy`可以查看dockerHub中的该镜像
2. 输入`docker pull tsieyy/bigdata-dev:v0.1` 拉取对应镜像即可