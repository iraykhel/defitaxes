# Defi taxes

## Running app locally

### Mac OS version

```
git clone https://github.com/iraykhel/taxes.git
cd taxes
python3 -m venv venv
. venv/bin/activate
```

then go to app.py and change `FLASK_ENV=...` to `FLASK_ENV="development"`

then inside the virtual environment

```
pip install -r requirements.txt
python app.py
```

## Running app in production

go to app.py and change `FLASK_ENV=...` to `FLASK_ENV="production"`

...