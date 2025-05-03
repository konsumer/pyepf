I made [epf-collector](https://github.com/konsumer/epf-collector), but wanted to see if python/dask can do it faster (it can!)

```sh
# setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# install pbzip2, however you do that

# assuming your data files are in data/epf this will just extract them all
./import
```
