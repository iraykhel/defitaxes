# Defi taxes

## Running app locally

driver.py should run locally without much fussing, it has main loop implemented that imports transactions and calculates taxes on them. 

For everything else you'll need Flask set up, app.py is flask's app entry point.

Main databases (db.db and addresses.db) are mostly empty here. They will be populated by the software automatically as it downloads coingecko rates and looks up addresses on scanners. Production db sizes are over 6 gig/0.5 gig, don't fit on github.