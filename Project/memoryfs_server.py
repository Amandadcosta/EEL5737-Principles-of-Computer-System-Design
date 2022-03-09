import pickle, logging
import argparse
import hashlib

# For locks: RSM_UNLOCKED=0 , RSM_LOCKED=1 
#RSM_UNLOCKED = bytearray(b'\x00') * 1
#RSM_LOCKED = bytearray(b'\x01') * 1

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)

class DiskBlocks():
  def __init__(self, total_num_blocks, block_size):
    # This class stores the raw block array
    self.block = []                                            
    # Initialize raw blocks 
    for i in range (0, total_num_blocks):
      self.block.insert(i,bytearray(block_size))



def ChecksumCalculation(Rawdata):
  #print(RawBlocks.block[block_number])
  actual_data = bytes(str(Rawdata),encoding='utf')
  hashed_value = bytearray(hashlib.md5(actual_data).digest())
  return hashed_value
  
def ChecksumPosition(block_number):
  index_of_blk = block_number//CHECKSUM_PER_BLOCK
  index_inside_block = block_number%CHECKSUM_PER_BLOCK
  return index_of_blk, index_inside_block
    
def corruptblock(block_number):
  c_data="Corrupted data stream".encode('utf-8')
  corrruptdata=bytearray(c_data.ljust(BLOCK_SIZE,b'\x00'))
  #print(corrruptdata)
  return corrruptdata


# This class stores the checksum 
class DiskChecksums():
  def __init__(self,CHECKSUM_SIZE):
    self.block1=[]
    # Initailizing checksum blocks
    for i in range(0,TOTAL_NUM_BLOCKS):
      self.block1.append(ChecksumCalculation(bytearray(BLOCK_SIZE)))
      #for j in range(0, CHECKSUM_PER_BLOCK):
        #self.block1[i].insert(j,bytearray(CHECKSUM_SIZE))


if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-sid', '--sid', type=int, help='an integer value')
  ap.add_argument('-cblk', '--cblk', type=int, help='an integer value')
  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks') 
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT=args.port
  else:
    print('Must specify port number')
    quit()
    
  if args.sid>=0:
    sid = args.sid
  else:
    print('Must specify valid Server ID')
    quit()

  #if args.cblk:
   # CORRUPT_BLOCK_NUM= args.cblk
  
  
  CHECKSUM_SIZE = 16
  CHECKSUM_PER_BLOCK = BLOCK_SIZE//CHECKSUM_SIZE #128//16=8
  
  TOTAL_CHECKSUM_BLOCK = TOTAL_NUM_BLOCKS //CHECKSUM_PER_BLOCK # 256//8=32
  
  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE)
  ChecksumsBlocks = DiskChecksums(CHECKSUM_SIZE)
  

    
  

  def Get(block_number):
    #index_of_blk,index_inside_block = ChecksumPosition(block_number)
    stored_checksum = ChecksumsBlocks.block1[block_number]
    #print(stored_checksum)
    computed_checksum = ChecksumCalculation(RawBlocks.block[block_number])
    #print(computed_checksum)
    if computed_checksum != stored_checksum:
      #error = bytearray(bytearray('Checksum did not match','utf-8').ljust(BLOCK_SIZE,b'\x00'))
      #data=RepairCorruptedBlock(block_number)
      return "error"
    return RawBlocks.block[block_number]
    
  def Put(block_number, data):
    if args.cblk:
      CORRUPT_BLOCK_NUM= args.cblk
      #print(CORRUPT_BLOCK_NUM)
      if block_number==CORRUPT_BLOCK_NUM:
        RawBlocks.block[block_number] = corruptblock(block_number)
      else:
      	RawBlocks.block[block_number]=bytearray(data.data)
        
    else:
      RawBlocks.block[block_number] = bytearray(data.data)
    
    checksum = ChecksumCalculation(bytearray(data.data))
    #index_of_blk,index_inside_block = ChecksumPosition(block_number)
    ChecksumsBlocks.block1[block_number]= checksum
    
    return 0
  
  def RSM(block_number):
    result = RawBlocks.block[block_number]
    # RawBlocks.block[block_number] = RSM_LOCKED
    RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
    return result
    
  def sidPORT(i,sid,PORT):
    if sid == i:
      server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler) 
      server.register_function(Get)
      server.register_function(Put)
      #server.register_function(RSM)
      #Run the server's main loop
      print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " at port" + str(i)+" "+str(PORT))
      server.serve_forever()	
    return 0
    
  for i in range(0,8):  
    sidPORT(i,sid,PORT)
  
  
  
  
  
  
  	
  
  

  

  

  

  

  

  

