The 01. agmkt_2021_2024.R files lay down an algorithm that appends all the crop prices and quantities for four years from 2020 to 2023 for all states of India. This data is daily data of the price and quantity of over three hundred agricultural crops in India and is reported daily. The code appends and further converts the daily data to monthly data. The daily quantity is then converted to monthly quantity by summing over all days in a month for each crop. However, the conversion of daily price to monthly price required quantity weighting. Finally, the cleaned dataset is a panel of all the crops for each month at the district level for four years. 

This code is generally written. One can use this code to extend it for other years as well.

