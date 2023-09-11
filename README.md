# Defi taxes

## Running app locally

You will need to rename sample.env to .env and populate it with API keys for a zillion third-party services, all of them (except debank) have a free version though. For debank you'll need to pay $200 :( Without debank balance check inspection won't work, but the rest should.

driver.py should run locally without much fussing, it has main loop implemented that imports transactions and calculates taxes on them. 

For everything else you'll need Flask set up, app.py is flask's app entry point.

Main databases (db.db and addresses.db) are mostly empty here. They will be populated by the software automatically as it downloads coingecko rates and looks up addresses on scanners. Production db sizes are over 6 gig/0.5 gig, don't fit on github.
