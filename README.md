# Defi taxes

## Running app locally

driver.py should run locally without much fussing, it has main loop implemented. For everything else you'll need Flask set up.
Main databases (db.db and addresses.db) are mostly empty here. They will be populated by the software automatically. Production db sizes are over 6 gig/0.5 gig.

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
