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

# assuming your data files are in data/epf this will just extract them all
./import

# if you just want to run it on a single file
pbzip2 -cd application.tbz | python ./epf2parquet.py out/application
```
