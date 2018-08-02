#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////
# script: s3_crawler.py
# author: Lincoln Harris
# date: 5.19
#
# Script for transfering cells from one s3 bucket to another (or a 
# group of query s3 buckets), according to a list of cells of interest. 
#
# 6.27: This has been optimized for parallel processing with GNU parallel. 
# Assuming you have GNU parallel installed, run like so:
#
#		parallel python3 s3_crawler ::: prefixList.csv
#
# where prefixList.csv is a file containing all of the s3 prefixes
# you want to search through
#
# s3_util library was written by James (THANK YOU)
#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////
# import s3_util
import os
import csv
import sys
import multiprocessing
import boto3
import botocore.exceptions

def prefix_gen(bucket, prefix, fn=None):
    """Generic generator of fn(result) from an S3 paginator"""
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')

    response_iterator = paginator.paginate(
            Bucket=bucket, Prefix=prefix
    )

    for result in response_iterator:
        if 'Contents' in result:
            yield from (fn(r) for r in result['Contents'])

# changes in here -- LJH
def get_files(bucket=None, prefix=None):
    # """Generator of keys for a given S3 prefix"""
    yield from prefix_gen(bucket, prefix, lambda r: r['Key'])

def copy_file(k):
    key, new_key = k
    try:
        s3c.head_object(Bucket=new_bucket, Key=new_key)
    except botocore.exceptions.ClientError:
        s3c.copy(CopySource={'Bucket': bucket, 'Key': key},
                 Bucket=new_bucket,
                 Key=new_key)

def copy_files(src_list, dest_list, b, nb, n_proc=16):
    """
    Copy a list of files from src_list to dest_list.
    b - original bucket
    nb - destination bucket
    """

    global s3c
    s3c = boto3.client('s3')

    global bucket
    bucket = b
    global new_bucket
    new_bucket = nb

    try:
        p = multiprocessing.Pool(processes=n_proc)
        p.map(copy_file, zip(src_list, dest_list), chunksize=100)
    finally:
        p.close()
        p.join()

#////////////////////////////////////////////////////////////////////
# getCellFile()
#	args: myPath -> raw os path to tCell local file of interest (.csv)
#	returns: fPath -> python path to file of interest
#
# 	Just letting python know where our cellFile is found
#////////////////////////////////////////////////////////////////////
def getCellFile(myPath):
    # the actual file name
	fName = os.path.basename(myPath) 
    # the short path to that file
	sPath = os.path.dirname(myPath)
    # get full path to file
	fPath = os.path.join(sPath,fName)
    
	return fPath

#////////////////////////////////////////////////////////////////////
# getCellSet()
#	args: myFile -> complete path to a local file (.csv) containing all
#					of the tCells we're interested in grabbing. 
#	returns: tSet -> a set (like a list, but hashed) of all the items
#				     in the input file
#	
#	Defining a set of all our query cells. 
#////////////////////////////////////////////////////////////////////
def getCellSet(myFile):
	with open(myFile) as f:
		rdr = csv.reader(f)
		tSet = set()
		for item in rdr:
			tSet.add(item[0])
	return tSet

#////////////////////////////////////////////////////////////////////
# engine()
#	args: myPrefix -> query bucket prefix (s3) to get all subdirs within
#		  myTCellSet -> set of tCells to search for
#		  myBucket -> name of the s3 parent bucket to search through 
#	returns: 
#		  f_to_move -> list of files in query bucket prefix to be moved
#		  res_files -> list of names of destination files...where we 
#					   want our new files to be stored
# 
#	Searches through s3 buckets for files contained w/in a list
#////////////////////////////////////////////////////////////////////
def engine(myPrefix, myTCellSet, myBucket):
	f_to_move = []
	res_files = []
	for query_dir in get_files(bucket=myBucket, prefix=myPrefix):
		dir_split = query_dir.split("/")
		if len(dir_split) > 3:
			query_cell_extra = dir_split[3]
			query_cell_extra_split = query_cell_extra.split("_")
			query_cell = query_cell_extra_split[0] + '_' + query_cell_extra_split[1]
			#print(query_cell)
			if query_cell in myTCellSet:
				f_to_move.append(query_dir)
				myStr = dest_bucket + query_cell + '/' + dir_split[4]
				res_files.append(myStr)
    
	return f_to_move, res_files

#////////////////////////////////////////////////////////////////////
# moveFiles()
#	args: mv -> list of file names to move (source)
#		  rs -> list of files to move to (destination)
#		  source_prefix -> s3 bucket to move files from
#	returns: NONE
#
#	Peforms the actual move, from one s3 bucket to another
#////////////////////////////////////////////////////////////////////
def moveFiles(mv, rs, source_prefix):
	copy_files(mv, rs, source_prefix, sys.argv[3], n_proc=64)

#////////////////////////////////////////////////////////////////////
# driverLoop()
#	args: pList -> list of prefixes w/in a parent s3 bucket
#		  tcSet -> set of cells to search for
#		  cBucket -> parent s3 bucket
#	returns: NONE	
# 
#	Main logic here. Calls engine() to find files to move within query
#	bucket, then calls moveFiles() to do the move. 
#////////////////////////////////////////////////////////////////////
def driverLoop(prefix, tcSet, cBucket):
	# print(tcSet)
	# for i in range(len(pList)):
	prefix = 'fastqs/' + prefix
	driver_out = engine(prefix, tcSet, cBucket)
	files_to_move = driver_out[0]
	results_files = driver_out[1]
	print(len(files_to_move))
	print("moving...")
	moveFiles(files_to_move, results_files, cBucket)
	# return

#////////////////////////////////////////////////////////////////////
# main()
# 
#	scaffold for initiating lists & loops. Calls getCellFile() and 
# 	getTCellSet() to define list of tCells to search for, then defines 
# 	lists of s3 prefixes to search our three parent s3 buckets for. 
# 	Then calls driverLoop to do all of the actual work. 
#
#////////////////////////////////////////////////////////////////////
  
global dest_bucket

cellSet = getCellSet(sys.argv[2])
dest_bucket = 'chimerCellFiles/'

inputFile = sys.argv[1]

prefixList = []
with open(inputFile) as f:
	input_rdr = csv.reader(f)
	for item in input_rdr:
		prefixList.append(item[0])

print(" ")
print("STARTING")
procs = []
for prefix in prefixList:
	p = multiprocessing.Process(target=driverLoop, args=(prefix, cellSet, 'czbiohub-seqbot',))
	procs.append(p)
	p.start()
for p in procs:
	p.join()

# driverLoop(prefixList, cellSet, 'czbiohub-seqbot')
print("done!")

#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////
