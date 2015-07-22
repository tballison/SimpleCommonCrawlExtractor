# SimpleCommonCrawlExtractor
Simple wrapper around IIPC Web Commons to take a literal warc.gz and extract standalone binaries

This is only meant for toy (=single box) processing of commoncrawl data.
Please, please, please use [Behemoth](https://github.com/DigitalPebble/behemoth) or another Hadoop framework for
actual processing!!!

I'm under no illusion that this capability doesn't already exist.  The
inspiration for this came Dominik Stadtler's project
[CommonCrawlDocumentDownload](https://github.com/centic9/CommonCrawlDocumentDownload).

Many thanks to Julien Nioche for running an initial dump of CommonCrawl data
with Behemoth!

Steps:
1. Find announcement of fresh data (e.g.
[link](http://blog.commoncrawl.org/2015/07/may-2015-crawl-archive-available/)
2. Download gz file of links for WARC files (e.g.
[link](https://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-data/CC-MAIN-2015-22/warc.paths.gz)
3. Download some of WARC files (< ~1GB each)
4. Run the extractor: nohup java -cp cc-extractor-0.0.1.jar org.tallison.commoncrawl.ExtractorCLI 
 -w /data1/public/archives/commoncrawl2 -o /data2/docs/commoncrawl2 
-ih "pdf|outlook|vnd.openxmlformats" 
-id "pdf|outlook|msoffice|tika-ooxml|rtf" -iu msg &

-ih -- include httpheader mimes that match this pattern
-id -- include Tika-detecte mimes that match this pattern
-iu -- include file extensions scraped from urls that match this pattern.
