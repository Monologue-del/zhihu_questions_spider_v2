# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import datetime
from pymongo import MongoClient
from zhihu.items import ZhihuQuestionItem
from zhihu.items import ZhihuAnswerItem
from zhihu.util.common import extract_num
from zhihu.settings import SQL_DATETIME_FORMAT, SQL_DATE_FORMAT


class ZhihuPipeline(object):

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI')
        )

    def open_spider(self, spider):
        # 连接到数据库
        self.client = MongoClient(self.mongo_uri)
        # 连接到指定的数据库
        zhihuDB = self.client['zhihuDB']
        # 连接到指定表
        self.que_collection = zhihuDB['questions_v1']
        self.ans_collection = zhihuDB['answers_v1']

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        # 存储question到MongoDB
        if isinstance(item, ZhihuQuestionItem):
            zhihu_id = item['zhihu_id'][0]
            try:
                topics = ','.join(item['topics'])
            except:
                topics = ''
            url = item['url'][0]
            title = item['title'][0]
            try:
                content = ''.join(item['content'])
            except:
                content = ''
            answer_num = item['answer_num'][0]
            comments_num = extract_num(item['comments_num'][0])
            crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

            if len(item["watch_user_num"]) == 2:
                watch_user_num = int(item["watch_user_num"][0].replace(',', ''))
                click_num = int(item["watch_user_num"][1].replace(',', ''))
            else:
                watch_user_num = int(item["watch_user_num"][0].replace(',', ''))
                click_num = 0

            que_dic = {
                'zhihu_id': zhihu_id,
                'topics': topics,
                'url': url,
                'title': title,
                'content': content,
                'answer_num': answer_num,
                'comments_num': comments_num,
                'watch_user_num': watch_user_num,
                'click_num': click_num,
                'crawl_time': crawl_time
            }
            self.que_collection.update({'id': que_dic['zhihu_id']}, {'$set': que_dic}, True)
            print('question', zhihu_id, '爬取成功')
            return item
        # 存储answer到MongoDB
        elif isinstance(item, ZhihuAnswerItem):
            self.ans_collection.update({'id': item['zhihu_id']}, {'$set': item}, True)
            print('answer', item['zhihu_id'], '爬取成功')