NK: Old instructions for wbkg mysql-spreadsheet synchronizer 

1. Open ApiNATOMY editor at open-physiology-viewer.surge.sh
2. Open the latest WBKG model:
https://docs.google.com/spreadsheets/d/1BcUBExy-kk-03ceeFuuz8comX3-O0xL1_owTkmZQTCU/edit#gid=780199172
3. Open Material Editor, edit the model. Press "Apply changes" button on the right menu 
   to save changes and propagate to the main view. 
4. Press Export Excel model, the modified model called wbrcm-converted.xlsx
5. Open a test spreadsheet https://docs.google.com/spreadsheets/d/1PmuQTQZ2xf1EJRBREO6ACd59nkNtVn_6glV4AaROvGk/edit#gid=365583290
   (You can use URL in #2 after testing has been done).
6. Press File -> Import -> Upload, and select the downloaded file.
7. Download mysql-to-xlsx-wbkg.py from https://github.com/open-physiology/apinatomy-converter
8. Place file service_account.json and wbkg_db.json to the folder 'data/'
9. Run the script to see and synchronize the changes between mySQL and the spreadsheet     