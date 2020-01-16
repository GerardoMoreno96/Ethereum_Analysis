# Ethereum_Analysis
## Analysis of Ethereum transactions and Smart Contracts

The goal of this coursework is to apply the techniques covered in the first half of Big Data Processing to analyse the full set of transactions which have occurred on the Ethereum network; from the first transactions (14-02-2016) till 30-06-2019. 

### DATASET OVERVIEW

Ethereum is a blockchain based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mint their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralised, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.

A subset of the data available on BigQuery is provided at the HDFS folder. The blocks, contracts and transactions tables have been pulled down and been stripped of unneeded fields to reduce their size. There is set of scams, both active and inactive, run on the Ethereum network via etherscamDB which is available on HDFS.

### PART A. TIME ANALYSIS

Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

![](images/partA-topten.png)


### PART B. TOP TEN MOST POPULAR SERVICES

Evaluate the top 10 smart contracts by total Ether received.

### PART C. DATA EXPLORATION
#### Popular Scams: 
Utilising the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time? 