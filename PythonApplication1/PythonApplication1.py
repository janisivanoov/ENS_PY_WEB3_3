import json
import urllib.request
#from mysql.connector import cursor
import web3

from web3 import Web3
from ens.auto import ENS

# Initialise WEB3 ENS
w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/e1aff836d3a64d6aba0f028217da381f"))
ns = ENS.fromWeb3(w3)

import mysql.connector
from mysql.connector import errorcode

# Initializing parameters
my_config = {
  'user': 'ens_user',
  'password': 'ens_password',
  'host': 'localhost',
  'database': 'ens',
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


# Get transactions
req = urllib.request.urlopen('https://api.etherscan.io/api?module=account&action=txlist&address=0x084b1c3c81545d370f3634392de611caabff8148&sort=desc&apikey=7I39Q4ZZ6SER7ZZTKQMNGYHD3UTZ6BSQ32')
resp = req.read()
tr = json.loads(resp)
addresses = []
i = 0
for txh in tr["result"]:
    
    addresses.append(Web3.toChecksumAddress(txh["from"]))
    i += 1
    if i == 1000:
        break


names = rrcontract.functions.getNames(addresses).call()

i = 0
for n in names:
    print(addresses[i] + "---" + n)
    i += 1

# initializing MySQL connection
try:
  my_cn = mysql.connector.connect(**my_config)
except mysql.connector.Error as err:
    print(err)
    quit()
else:
  print ("Connected to the Database")

cursor =  my_cn.cursor()

# Function Reads First block    

def getBlock():
    # read block number
    q_block = "SELECT Max(block) from ens.rev_registry"
    cursor.execute(q_block)
    for (block) in cursor:
        start_block = block
    if start_block == (None,):
        start_block = (0,)
    return start_block[0]

# Function updates last blosk
def updateBlock(block):
    u_block = "update ens.block set block = %s"
    cursor.execute(u_block,[block])
    my_cn.commit()

# Function updates reverse registry table
def updateName(domain, address, block):
    i_name = "insert into rev_registry (addr, name, block) values (%s, %s, %s)"
    u_name = "update rev_registry set name = %s, block=%s where addr = %s"
    d_name = "delete from rev_registry where addr = %s"
    if domain == "None":
        # delete name from the registry
        cursor.execute(d_name, [address])
        my_cn.commit()
    else:
        # insert or update
        
        try:
            # attempt to insert
            cursor.execute(i_name, [address, domain, block])
        except mysql.connector.Error as err:
            # update if record is there
            if err.errno == 1062:
                cursor.execute(u_name, [domain, block, address])
            else:
                print (err)
                quit()
        finally:
            my_cn.commit()

# UpdateBlock (end_block)

for txh in tr["result"]:
    domain = ns.name(txh["from"])
    print(txh["from"], " --- ", domain, " --- ", txh["blockNumber"])
    # update
    updateName(str(domain), str(txh["from"]), str(txh["blockNumber"]))

cursor.close()
my_cn.close()