# Ethereum_Analysis
## Analysis of Ethereum transactions and Smart Contracts

The goal of this coursework is to apply the techniques covered in the first half of Big Data Processing to analyse the full set of transactions which have occurred on the Ethereum network; from the first transactions (14-02-2016) till 30-06-2019. 

### DATASET OVERVIEW

Ethereum is a blockchain based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mint their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralised, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.

A subset of the data available on BigQuery is provided at the HDFS folder. The blocks, contracts and transactions tables have been pulled down and been stripped of unneeded fields to reduce their size. There is set of scams, both active and inactive, run on the Ethereum network via etherscamDB which is available on HDFS.

### PART A. TIME ANALYSIS

<u>Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.</u>

The month with the highest number of transactions was 2018-01 with a total of 33,504,270
transactions. The rest of transactions for each month can be appreciated in the following bar plot:

![](images/partA-topten.png)

### PART B. TOP TEN MOST POPULAR SERVICES

</u>Evaluate the top 10 smart contracts by total Ether received.</u>

```
0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444	84155100809965865822726776
0xfa52274dd61e1643d2205169732f29114bc240b3	45787484483189352986478805
0x7727e5113d1d161373623e5f49fd568b4f543a9e	45620624001350712557268573
0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef	43170356092262468919298969
0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8	27068921582019542499882877
0xbfc39b6f805a9e40e77291aff27aee3c96915bdd	21104195138093660050000000
0xe94b04a0fed112f3664e45adb2b8915693dd5ff3	15562398956802112254719409
0xbb9bc244d798123fde783fcc1c72d3bb8c189413	11983608729202893846818681
0xabbb6bebfa05aa13e908eaa492bd7a8343760477	11706457177940895521770404
0x341e790174e3a4d35b65fdc067b6b5634a61caea	8379000751917755624057500
```

### PART C. DATA EXPLORATION
#### Popular Scams: 
Utilising the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time? 

![](images/partC1.png)
![](images/partC2.png)
![](images/partC3.png)