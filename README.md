This will convert an EPF file to parquet files.

I made [epf-collector](https://github.com/konsumer/epf-collector), but wanted to see if python/dask can do it faster (it can!)

[pbzip2](https://github.com/ruanhuabin/pbzip2) is highly recommended. it's a lot faster than bunzip2.

```sh
# setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# install pbzip2, however you do that
# here is what I did, on mac:
brew install pbzip2

# assuming your data files are in data/epf this will just extract them all, then import them into a duckdb (if that is installed)
./import

# if you just want to run it on a single file
pbzip2 -cd application.tbz | python ./epf2parquet.py out/application
```


## ideas

I think it could be further improved with a few things:

- use `export_date` for partition on all data (for historical)
- use primary-keys as sequence, if possible (so it's easier to find the correct parquet by primary)
- normalize more: like application has artist_name and also linkage in artist_application, same with title/description and other stuff in application_detail (could be partitioned by storefront/language)
- better handling of bad rows. Some rows have \1 in the field-data, and currently it's just skipped (~20 in application, and ~60 in application_detail)
- `application_price` has this: `'utf-8' codec can't decode byte 0x80 in position 124: invalid start byte`
- download script, similar to what I have in [epf-collector](https://github.com/konsumer/epf-collector)
- nicer progress-bar
- verify data. I notice I get a few more skips (like I get 1 on artist) so it should all be compared with [epf-collector](https://github.com/konsumer/epf-collector)
- direct download/update (to s3)?
