from azure.storage.blob import BlobService
import datetime
import time

account_name = 'mdp2015'
account_key = 'oVNPEj1aA0U3/bjHSV6/au34x+fDTdWI/BIIO2EncWV/bUTDy5P4jHiXSWBRakfRrpFLgeRtvHFzOzAOfBHbcA=='
container_name = 'data1'
history_file = 'History'
subscribe_file = 'Subscribe'


def getBlobList(blob_service):
	blobs = []
	marker = None
	while True:
	    batch = blob_service.list_blobs(container_name, marker=marker)
	    blobs.extend(batch)
	    if not batch.next_marker:
	        break
	    marker = batch.next_marker
	return [blob.name for blob in blobs]

def removeHeader(blob_text):
	return blob_text[blob_text.index('\n')+1:]

def blobToText(blob_service, blob_list):
	text_list = []
	for blob_name in blob_list:
		new_text = blob_service.get_blob_to_text(container_name, blob_name) + '\n'
		text_list.append(new_text)
	return text_list

def initHistoryFile(blob_service, text_list):
	if text_list:
		history_text = text_list[0]
		for i in xrange(1,len(text_list)):
			history_text += removeHeader(text_list[i])
		blob_service.put_block_blob_from_text(container_name, history_file, history_text)

def updateHistoryFile(blob_service, text_list):
	if text_list:	
		history_text = blob_service.get_blob_to_text(container_name, history_file)
		for text in text_list:
			history_text += removeHeader(text)
		blob_service.put_block_blob_from_text(container_name, history_file, history_text)	

def updateSubscribeFile(blob_service, text_list):
	if text_list:
		subscribe_text = text_list[0]
		for i in xrange(1,len(text_list)):
			subscribe_text += removeHeader(text_list[i])
		blob_service.put_block_blob_from_text(container_name, subscribe_file, subscribe_text)

def clearAll(blob_service):
	pass

def uploadFile(blob_service, file_name):
	blob_service.put_block_blob_from_path(container_name, file_name, file_name, max_connections=5)

def main():
	blob_service = BlobService(account_name, account_key)
	blob_list = getBlobList(blob_service)
	if history_file not in blob_list:
		initHistoryFile(blob_service, blobToText(blob_service, blob_list))
	if subscribe_file not in blob_list:
		if history_file in blob_list:
			blob_list.remove(history_file)
			print blob_list
		updateSubscribeFile(blob_service, blobToText(blob_service, blob_list))	
	while True:
		new_blob_list = getBlobList(blob_service)
		new_blob_list.remove(history_file)
		new_files = [file_name for file_name in new_blob_list if file_name not in blob_list]
		print new_files
		if new_files:
			updateSubscribeFile(blob_service, blobToText(blob_service, new_files))
			print 'update subscribe data'
		else:
			print 'nothing to update'
		blob_list = new_blob_list
		time.sleep(5)

if __name__ == '__main__':
	main()