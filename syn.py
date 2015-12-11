from genTable import *
import os

account_name = 'mdp2015'
account_key = 'oVNPEj1aA0U3/bjHSV6/au34x+fDTdWI/BIIO2EncWV/bUTDy5P4jHiXSWBRakfRrpFLgeRtvHFzOzAOfBHbcA=='
container_name = 'data1'

def synchronize():
	blob_service = BlobService(account_name, account_key)
	local_files = os.listdir('data')
	remote_files = getBlobList(blob_service)
	upload_list = [file_name for file_name in local_files if file_name not in remote_files]
	for file_name in upload_list:
		if file_name[0] != '.':
			blob_service.put_block_blob_from_path(container_name, file_name, 'data/'+file_name, max_connections=5)


def main():
	synchronize()

if __name__ == '__main__':
	main()