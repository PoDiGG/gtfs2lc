# GTFS to Linked Connections

Transforms a GTFS file towards a directed acyclic graph of "connections".

A connection is the combination of a departure and its successive arrival of the same trip. 
Our goal is to retrieve a list of connections that is sorted by departure time, better known as a Directed Acyclic Graph. This way, routeplanning algorithms can be performed.

## Install it

```
npm install -g gtfs2lc
```

## Use it

First, __unzip__ your GTFS file to a place on your disk using e.g., `unzip gtfs.zip -d /tmp`

Now, we need to make sure that a couple of files are ordered in a specific fashion, not required by the GTFS spec:
 * __stop_times.txt__ must be ordered by `trip_id` and `stop_sequence`. Mind that the number of the columns are also not standardized by GTFS and you might need to tweak the sort command in this repo.
 * __calendar.txt__ must be ordered by `service_id`.
 * __calendar_dates.txt__ must be ordered by `service_id`.
 
We've enclosed a bash script which ensures this for you. It isn't perfect however and may not return the desired result. You can run this bash script using `gtfs2lc-sort $pathname`.

If you've ensured this, you can use this tool on the command line as follows:

```bash
gtfs2lc /path/to/extracted/gtfs -f csv --startDate 20151101  -e 20160101
```

You also can load the data into mongodb in extended json-ld format as follows:

```bash
gtfs2lc /path/to/extracted/gtfs -f mongold -b baseUris.json | mongoimport -c myconnections
```

Mind that only MongoDB starting version 2.6 is supported.

For more options, check `gtfs2lc --help`

## Enrichment step

The output data can automatically be enriched with this tool when adding the `-r` or `--enrich` option.
This will add additional stop information that is available in `stops.txt` like stop name, platform code, station, longitude and latitude.
The station information is retrieved from the iRail API, and will emit information like station name, country, longitude and latitude.
Next to that, each station will be linked with a LinkedGeoData node (if a corresponding Station node can be found).

Currently, this enrichment step only works optimally for Belgian railway data, because the iRail API is used.
When this enrichment step is done for data from other countries, the station information will not be generated.

## How it works (for contributors)

We convert `stop_times.txt` to a stream of connection rules. These rules need a certain explanation about on which days they are running, which can be retrieved using the `trip_id` in the connection rules stream.

At the same time, we process `calendar_dates.txt` and `calendar.txt` towards a binary format. It will contain a 1 for the number of days from a start date for which the service id is true.

In the final step, the connection rules are expanded towards connections by joining the days, service ids and rules.

## Not yet implemented:

At this moment we've only implemented a conversion from the Stop Times to connections. However, in future work we will also implement a system for describing trips and routes, a system for transit stops and a system for transfers.

Furthermore, also `frequencies.txt` is not supported at this time. We hope to support this in the future though.

## Authors

Pieter Colpaert - pieter.colpaert@ugent.be
