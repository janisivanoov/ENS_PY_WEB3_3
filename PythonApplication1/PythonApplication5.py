import json
from logging import error, exception
import urllib.request
import web3
import hashlib

from web3 import Web3
from ens.auto import ENS

# Initialise WEB3 ENS
w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/e1aff836d3a64d6aba0f028217da381f"))
ns = ENS.fromWeb3(w3)

import mysql.connector
from mysql.connector import errorcode

eth_key = "7I39Q4ZZ6SER7ZZTKQMNGYHD3UTZ6BSQ32"
#eth_contract = "0x084b1c3c81545d370f3634392de611caabff8148"
#eth_contract = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41"

maxcount = 100

# Initializing parameters
my_config = {
  'user': 'ens_user',
  'password': 'ens_password',
  'host': 'localhost',
  'database': 'ens4',
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
    #cursor.reset()
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
    i_name = "insert into rev_registry (addr, name, contract, block, hash) values (%s, %s, %s, %s, %s)"
    u_name = "update rev_registry set name = %s, contract = %s, block=%s, hash = %s where addr = %s"
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
                cursor.execute(i_name, [address, domain, eth_contract, block, hashlib.sha512(domain.encode('utf-8')).hexdigest()])
            except mysql.connector.Error as err:
                # update if record is there
                if err.errno == 1062:
                    cursor.execute(u_name, [domain, eth_contract, block, address, hashlib.sha512(domain.encode('utf-8')).hexdigest()])
                else:
                    print (err)
                    quit()
            finally:
                my_cn.commit()

def updateNames(addresses, blocks, contracts):
    try:
            names = rrcontract.functions.getNames(addresses).call()
            ii = 0
            for n in names:
                print(addresses[ii] + "---" + n)
                updateName(str(n), str(addresses[ii]), str( contracts[ii]), str(blocks[ii]))
                ii += 1
    except BaseException as err:
            print("Exception. Cannot resolve names  " + str(err))

# initializing MySQL connection
try:
  my_cn = mysql.connector.connect(**my_config)
except mysql.connector.Error as err:
    print(err)
    quit()
else:
  print ("Connected to the Database")

cursor =  my_cn.cursor() 

# Making a consolidated array of addresses

# Read contracts into array
contracts = []
c_sql = "select address from contracts"
cursor.execute(c_sql)
for c in cursor:
    contracts.append(c)


aaa = []
bbb = []
ccc = []
i=0
for c in contracts:   
    print("Processing " + c[0])
    # Get transactions
    #req = urllib.request.urlopen('https://api.etherscan.io/api?module=account&action=txlist&address=0x084b1c3c81545d370f3634392de611caabff8148&sort=desc&apikey=7I39Q4ZZ6SER7ZZTKQMNGYHD3UTZ6BSQ32')
    url ='https://api.etherscan.io/api?module=account&action=txlist&address='+c[0]+'&startblock='+str(getBlock(c[0]))+'&sort=asc&apikey='+eth_key

    req = urllib.request.urlopen(url)
    resp = req.read()
    tr = json.loads(resp)
    for txh in tr["result"]:
        aaa.append(Web3.toChecksumAddress(txh["from"]))
        bbb.append(txh["blockNumber"])
        ccc.append(c[0])
        i += 1


b = 0
# loop with maxcount step
while (b+maxcount<len(aaa)):
    updateNames(aaa[b:b+maxcount], bbb[b:b+maxcount], ccc[b:b+maxcount])
    b += maxcount

updateNames(aaa[b:len(aaa)-1], bbb[b:len(bbb)-1], ccc[b:len(ccc)-1])

# Fix duplications
print("Cleaning duplicates...")

# Find Duplications
c_sql = "select name from dup_names"
cursor.execute(c_sql)
da = []
dn = []
for c in cursor:
    # find true address
    addr = ns.address(c[0])
    print (c[0] + '---' + addr)
    da.append(addr)
    dn.append(c[0])

# Delete duplications
d_name = "delete from rev_registry where name = %s and addr != %s "
i=0
for address in da:
    cursor.execute(d_name, [ dn[i], address ])
    my_cn.commit()
    i+=1


        
# Close the cursor and the connection
cursor.close()

my_cn.close()