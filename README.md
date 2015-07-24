# SimpleCommonCrawlExtractor
Simple wrapper around IIPC Web Commons to take a literal warc.gz and extract standalone binaries

This is only meant for toy (=single box) processing of commoncrawl data.
Please, please, please use [Behemoth](https://github.com/DigitalPebble/behemoth) or another Hadoop framework for
actual processing!!!

I'm under no illusion that this capability doesn't already exist...probably 
even with IIPC's Web Commons!

The inspiration for this came from Dominik Stadler's
[CommonCrawlDocumentDownload](https://github.com/centic9/CommonCrawlDocumentDownload).

This tool requires a local repository of warcs...it does not do streaming processing...
did I mention "toy" above?

This tool allows for selection (inclusion or exclusion)
of records by http-header mime type, Tika-detected mime type and/or
file extension scraped from the target URL.

Many thanks to Julien Nioche for running an initial dump of CommonCrawl data
with Behemoth!

Steps:

1. Find announcement of fresh data (e.g.
[link](http://blog.commoncrawl.org/2015/07/may-2015-crawl-archive-available/) )

2. Download gz file of links for WARC files (e.g.
[link](https://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-data/CC-MAIN-2015-22/warc.paths.gz))

3. Download some WARC files (< ~1GB each)...wget -i list_of_warcs.txt

4. Run the extractor: nohup java -cp cc-extractor-0.0.1.jar org.tallison.commoncrawl.ExtractorCLI 
 -w /data1/public/archives/commoncrawl2 -o /data2/docs/commoncrawl2 
-ih "pdf|outlook|vnd.openxmlformats" 
-id "pdf|outlook|msoffice|tika-ooxml|rtf" -iu msg &
* -w warc file or warc directory
* -o output directory
* -ih -- include httpheader mimes that match this pattern
* -id -- include Tika-detect mimes that match this pattern
* -iu -- include file extensions scraped from urls that match this pattern.

This will output files named with their base32-encoded sha1 hashes that come 
from the warcs.  If an extension can be discovered or computed from the mime
type, the file will include that extension.

By default, this skips truncated files, but if you want to include them, 
add the -t parameter.

Test cases and logging?  Y, those are coming...