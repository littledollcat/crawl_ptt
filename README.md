# crawl_ptt

# 使用scrapy, scrapy-redis 建置分散式爬蟲系統


# 安裝：
+ docker-compose up -d
+ docker ps
+ docker exec -it [crawler docker id] bash
+ cd crawl_ptt
+ scrapy crawl ptt -a category='movie' -a startTime='2020-03-23' -a endTime='2020-03-24'

category 可指定ptt板
startTime與endTime 僅接受 2020-03-23 日期格式
