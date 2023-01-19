# Defi taxes

## Running app locally

driver.py should run locally without much fussing, it has main loop implemented that imports transactions and calculates taxes on them. 

For everything else you'll need Flask set up, app.py is flask's app entry point.

Main databases (db.db and addresses.db) are mostly empty here. They will be populated by the software automatically as it downloads coingecko rates and looks up addresses on scanners. Production db sizes are over 6 gig/0.5 gig, don't fit on github.

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
