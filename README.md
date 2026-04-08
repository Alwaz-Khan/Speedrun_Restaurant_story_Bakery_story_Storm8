🌪️ Bakery Story / Restaurant Story XP/Profit Farming

This project leverages data from
👉 https://stm.gamerologizm.com/

It is designed to work for both Restaurant Story and Bakery Story, developed by Storm8 Studios.

The goal of this pipeline is to identify the best recipes to farm for XP and profit, based on the time interval when a player plans to return to the game.

🚀 Overview

This project implements a full ETL pipeline:

Extract → Scrapes recipe and appliance data
Transform → Processes and structures the data
Load → Stores data in PostgreSQL and Google Sheets
✨ Features
📥 Recipe extraction
🔧 Appliance extraction
🗄️ PostgreSQL data loading
🖼️ Image downloading
📊 Google Sheets integration


⚙️ Setup Instructions

1. Clone the repository
git clone <your-repo-url>
cd <your-project-folder>

2. Configure environment variables
Create a .env file using the template:
.env.example
Then fill in your credentials (DB, Google Sheets, etc.).

3. Install dependencies
pip install -r requirements.txt

4. Run the pipeline
python main.py


📂 Project Purpose: This pipeline helps players:
Optimize gameplay efficiency
Maximize XP gain
Improve profit per time cycle
Make data-driven decisions on recipe selection

⚠️ Disclaimer
This project uses publicly available data from the referenced website.
All credit for raw data goes to the original source.

🧠 Future Improvements (Optional Ideas)
2. Separate data transformation included in postgresql script into a separate script.
3. In case of multiple recipe qualifying for max xp, need to create a further subfilter where it then chooses the recipe which gives max profit and same criterio for max profit recipes as well.
4. postgresql takes the 4 files separately and the google sheet script first combines all the 4 scripts into 1 master and then upload it, can separate the combined file from google sheet script and use the same to upload to postgresql, but that would merge the 2 separate sources of truth that is maintained currently.
5. this software requires the person to create a google sheet api key and credentials, can direclty prepare an output excel before sending the data to postgresql and google sheet to be published there. 
6. The github website: make the top bar scalable and use it for self advertisement, the make the cards auto fit the width, try to put picture of recipe for more clarity along with the names