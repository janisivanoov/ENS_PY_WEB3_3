import json
from logging import error, exception
import urllib.request
import web3

from web3 import Web3
from ens.auto import ENS

# Initialise WEB3 ENS
w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/e1aff836d3a64d6aba0f028217da381f"))
ns = ENS.fromWeb3(w3)

import mysql.connector
from mysql.connector import errorcode

eth_key = "7I39Q4ZZ6SER7ZZTKQMNGYHD3UTZ6BSQ32"
#eth_contract = "0x084b1c3c81545d370f3634392de611caabff8148"
eth_contract = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41"

maxcount = 100

# Initializing parameters
my_config = {
  'user': 'ens_user',
  'password': 'ens_password',
  'host': 'localhost',
  'database': 'ens2',
  'raise_on_warnings': True
}

rrabi = [
  {
    "inputs": [
      {
        "internalType": "contract ENS",
        "name": "_ens",
        "type": "address"
      }
    ],
    "stateMutability": "nonpayable",
    "type": "constructor"
  },
  {
    "inputs": [
      {
        "internalType": "address[]",
        "name": "addresses",
        "type": "address[]"
      }
    ],
    "name": "getNames",
    "outputs": [
      {
        "internalType": "string[]",
        "name": "r",
        "type": "string[]"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  }
]

rrcontract = w3.eth.contract(address = Web3.toChecksumAddress('0x3671aE578E63FdF66ad4F3E12CC0c0d71Ac7510C'), abi = rrabi)


# Function Reads First block    

def getBlock(contract):
    # read block number
    q_block = "SELECT Max(block) from rev_registry where contract = %s"
    cursor.execute(q_block, [contract])
    for (block) in cursor:
        start_block = block
    if start_block == (None,):
        start_block = (0,)
    return start_block[0]


# Function updates reverse registry table
def updateName(domain, address, eth_contract, block):
    i_name = "insert into rev_registry (addr, name, contract, block) values (%s, %s, %s, %s)"
    u_name = "update rev_registry set name = %s, contract = %s, block=%s where addr = %s"
    d_name = "delete from rev_registry where addr = %s"
    if domain != "":
        if domain == "None":
            # delete name from the registry
            cursor.execute(d_name, [address])
            my_cn.commit()
        else:
            # insert or update
        
            try:
                # attempt to insert
                cursor.execute(i_name, [address, domain, eth_contract, block])
            except mysql.connector.Error as err:
                # update if record is there
                if err.errno == 1062:
                    cursor.execute(u_name, [domain, eth_contract, block, address])
                else:
                    print (err)
                    quit()
            finally:
                my_cn.commit()

# initializing MySQL connection
try:
  my_cn = mysql.connector.connect(**my_config)
except mysql.connector.Error as err:
    print(err)
    quit()
else:
  print ("Connected to the Database")

cursor =  my_cn.cursor()

# Read contracts into array
contracts = []
c_sql = "select address from contracts"
cursor.execute(c_sql)
for c in cursor:
    contracts.append(c)


for eth_contract in contracts:
    
    print("processing " + str(eth_contract) + " contract")

    e = eth_contract[0]

    # Get transactions
    #req = urllib.request.urlopen('https://api.etherscan.io/api?module=account&action=txlist&address=0x084b1c3c81545d370f3634392de611caabff8148&sort=desc&apikey=7I39Q4ZZ6SER7ZZTKQMNGYHD3UTZ6BSQ32')
    url ='https://api.etherscan.io/api?module=account&action=txlist&address='+e+'&startblock='+str(getBlock(str(e)))+'&sort=asc&apikey='+eth_key
    req = urllib.request.urlopen(url)
    resp = req.read()
    tr = json.loads(resp)

    # Arrays with transaction addresses and blocks
    addresses = []
    blocks = []

    # Saving/resolving with maxcount step
    i=0
    mmm = maxcount
    for txh in tr["result"]:
        addresses.append(Web3.toChecksumAddress(txh["from"]))
        blocks.append(txh["blockNumber"])
        i += 1

        if i == mmm:
            mmm = mmm + maxcount
            try:
                names = rrcontract.functions.getNames(addresses).call()
                ii = 0
                for n in names:
                    print(addresses[ii] + "---" + n)
                    updateName(str(n), str(addresses[ii]), str(e),  str(blocks[ii]))
                    ii += 1
            except:
                print("Exception. Cannot resolve names  " + addresses[0])
            finally:
                addresses = []
                blocks = []


    # Flushing the reminng addresses
    ii = 0
    try:
       names = rrcontract.functions.getNames(addresses).call()

       for n in names:
            print(addresses[ii] + "---" + n)
            updateName(str(n), str(addresses[ii]), str(e), str(blocks[ii]))
            ii += 1
    except:
       print("Exception. Cannot resolve names  "  + addresses[0])

# Close the cursor and the connection
cursor.close()
my_cn.close()