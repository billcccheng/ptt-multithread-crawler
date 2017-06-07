# Ptt Crawler
This is a multithreaded crawler that crawls all the articles of the board you specified.

## How to use
```
python ptt_crawler.py <target-board> <thread-number>
```

e.g. Want to crawl ``tech_job`` board with 100 threads. 

```
python ptt_crawler.py tech_job 100
```

Each threads will store the data it crawled in 
their own files. ``Thread-1`` will store the data in ``data-1.json`` and so on.

## Special Thanks
Special thanks to [wy36101299](https://github.com/wy36101299/PTTcrawler) for making the ptt crawler

## ptt_crawler.py
Modified the original ptt_crawler.py to fit my needs.
