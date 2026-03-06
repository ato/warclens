# WarcLens

WarcLens is a reporting tool for analyzing web archive crawl outputs. It extracts structured metadata from WARC-based
crawls, stores it in DuckDB, and produces reusable reports on coverage, content, and crawl behavior.

## Usage

### Initializing a new analysis

    mkdir mycrawl
    cd mycrawl
    warclens init

### Initializing and importing WARC files

    warclens init warcs/*.warc.gz

### Running analysis

#### Show hosts report

    warclens hosts

#### Show hosts report for a site (host + subdomains)

    warclens hosts --site facebook.com
    warclens hosts --domain facebook.com

#### Sort hosts report

    warclens hosts --sort size

#### Show hosts report for a status code

    warclens hosts --status 200
    warclens hosts --status 4xx

#### Show media type report

    warclens mime

#### Show media type report for a specific host

    warclens mime --host connect.facebook.com

#### Show media type report for a site (host + subdomains)

    warclens mime --site facebook.com
    warclens mime --domain facebook.com

#### Sort media type report

    warclens mime --sort records

#### Show media type report for a status code

    warclens mime --status 200
    warclens mime --status 5xx

#### Show status code report

    warclens status

#### Show status code report for a host or site

    warclens status --host connect.facebook.com
    warclens status --site facebook.com
    warclens status --domain facebook.com

#### Show status code report filtered to a single status code

    warclens status --status 404
    warclens status --status 3xx

#### Sort status code report

    warclens status --sort status
