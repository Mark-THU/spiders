# 应用于Twitter全网爬虫
```
使用API，这种方法导致爬虫经常运行到一半，然后退出。
```

---

* 文件解释
> bloom_filter.py:布隆过滤器

> crawler.py：爬虫文件

> hash_function.py：11个哈希函数，在布隆过滤器中会用到

> tokens.txt：使用API要用到的15个密钥

* 命令行参数解释
> functions：全网爬虫/全网中文用户爬虫

> -host：数据库所在服务器ip

> -database：数据库名

> -proxy：代理，eg，127.0.0.1：1080

> -name：布隆过滤器名字

> -length：布隆过滤器位数组长度

> -number：布隆过滤器哈希函数个数

> -save_frequency：布隆过滤器位数组保存频率


* 更新
> 2019-08-24	
>> 加入命令行参数输入 

> 2019-08-26	
>> 更正逻辑问题，提高效率。 将searched-list与to-search-list中的用户都加入到bloom-filter中。

> 2019-09-02	
>> 遇到程序中途退出的问题，每次重新运行前，需要之前已经加入进bloom_filter中的id，重新add一遍。

> 2019-09-04	
>> 对于searched_list中的id，获取了id的profile之后要从表中删除，这样如果中途退出能够知道目前已经处理到哪一个id。

> 2019-10-21	
>> 使用自己实现的布隆过滤器，保存checkpoints用于程序中途退出后重新启动。

> 2019-10-22	
>> 1. checkpoints改用了limit，而不是between

>> 2. 逻辑修正，在中文全网用户爬虫中，将所有用户insert到过滤器中，而不仅仅是中文用户。

>> 3. API连续多次失效则等待一段时间，而不是一次失效就等待一段时间。（api_error_times）
