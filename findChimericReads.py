#/////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////
# script: findChimericReads.py
# author: Lincoln Harris, Gerry Meixiong
# date: 8/1/18
#
# This script searches a directory full of BLAST output files and writes
# files with the names of cells that contain chimeric reads ie. cells such 
# that for a given read pair, one read matches geneA, and the other matches 
# geneB. Does so for each possible pair of genes.
#
#/////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////
import os
import io
import sys
import itertools

#/////////////////////////////////////////////////////////////////////
# findReadPairs(): Searches for common elements between two lists
#	args ->
#		l1, l2 - two lists
#	returns ->
#		shared -  the common elements between these two lists
#
#/////////////////////////////////////////////////////////////////////
def findReadPairs(l1, l2):

	l1_set = set([line.split(" ")[0] for line in l1])
	l2_set = set([line.split(" ")[0] for line in l2])

	shared = l1_set.intersection(l2_set)

	return shared

#/////////////////////////////////////////////////////////////////////
# findHitSeqs(): Searches through a BLAST out file and returns a list 
#				of all of the sequences IDs that yielded a hit
#	args -> 
#		fileName - a BLAST output file (positive hit) to grab the 
#					sequence IDs corresponding to the hits for
#	returns ->
#		readsList - a list of the reads that yielded a BLAST hit
#
#/////////////////////////////////////////////////////////////////////
def findHitSeqs(fileName):
	readsList = []
	f = open(fileName, 'r')
	flines = f.readlines()
	for line in flines:
		# find corresponding read IDs
		if '>' in line:
			addLine = line.replace('\n', '')
			readsList.append(addLine)

	return readsList

#/////////////////////////////////////////////////////////////////////
# main():
#	Launches a loop over each subdir in the working directory, then
#	defines path to BLAST out files, calls findHitSeqs to get lists of
#	the sequences corresponding to BLAST hits for Alk or Eml4, then 
#	calls findReadPairs to search for chimeric reads. Prints to terminal
#	if chimeric read is found. 
#
#/////////////////////////////////////////////////////////////////////

blast_dir = sys.argv[1]
outdir = sys.argv[2]

geneList = ["alk", "ccdc6", "cd74", "cltc", "eml4", "ezr", "met", "ntrk2", "prkcb", "ret", "ros1", "rps6kb1", "slc34a2", "spns1", "strn", "tfg", "trim24", "trim33", "tubd1"]

for gene1, gene2 in itertools.combinations(geneList, 2):
	for cellDir in os.listdir(blast_dir):
		f1 = blast_dir + '/' + cellDir + '/' + gene1 + '_R1_blastOut'
		f2 = blast_dir + '/' + cellDir + '/' + gene1 + '_R2_blastOut'
		f3 = blast_dir + '/' + cellDir + '/' + gene2 + '_R1_blastOut'
		f4 = blast_dir + '/' + cellDir + '/' + gene2 + '_R2_blastOut'

		f1Hits = []
		f2Hits = []
		f3Hits = []
		f4Hits = []

		if 'Sequences producing significant alignments:' in open(f1).read():
			f1Hits = findHitSeqs(f1)
		if 'Sequences producing significant alignments:' in open(f2).read():
			f2Hits = findHitSeqs(f2)
		if 'Sequences producing significant alignments:' in open(f3).read(): 
			f3Hits = findHitSeqs(f3)
		if 'Sequences producing significant alignments:' in open(f4).read(): 
			f4Hits = findHitSeqs(f4)

		g1_hits = f1Hits + f2Hits
		g2_hits = f3Hits + f4Hits

		chimReads = findReadPairs(g1_hits, g2_hits)
		if(chimReads):
			with open(outdir+"/" + gene1+"_"+gene2+"_pairs.txt", 'a') as f:
				f.write("Found %d chimeric reads for cell %s\n" % (len(chimReads), cellDir))
				for read in chimReads:
					f.write('\t' + read +"\n")

#/////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////