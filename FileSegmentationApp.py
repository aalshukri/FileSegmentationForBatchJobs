"""
Python File Segmentation App

"""

import csv
import ConfigParser 
import sys
import os
import math
import time
import datetime

class FileSegmentationApp(object):	
	'FileSegmentationApp'
	pass


	"""
	" Constructor
	"""
	#def __init__():	
	#/contructor


	"""
	" test method
	"""
	def test(self):
		'test method'
		print("test")


	"""
	" main method
	"""
	def main(self,mainFileName,mainFileColumnId,mainFileNumChunks,supportingFileNames,supportingFileColumnIds):
		"""
		main

		This function controls the entire process.

		"""

		print("main")

		chunkedFileNames = []

		# Check if file chunking or not
		if mainFileNumChunks:
			# chunk main input file		
			chunkedFileNames = self.chunkFile(mainFileName,mainFileColumnId,mainFileNumChunks)
		else:			
			chunkedFileNames.append(mainFileName)
		
		#print(chunkedFileNames)

		

		# for each main file, chunked or not.
		print(" For main file(s)")
		for i in range(0,len(chunkedFileNames)):
			print("  "+str(i)+": "+chunkedFileNames[i])

			# create outputFileNames
			supportingFileOutNames=[]
			for item in supportingFileNames:
				supportingFileOutNames.append(item.replace('.csv','.seg'+str(i+1)+'.csv'))

			# get ids from main file			
			idDictionary = self.getFileIdsAsDictionary(chunkedFileNames[i],mainFileColumnId)
	
			for i in range(0,len(supportingFileNames)):			
				self.segmentFile(supportingFileNames[i],supportingFileOutNames[i],supportingFileColumnIds[i],idDictionary)

		



	"""
	" getFileIdsAsDictionary
	"""
	def getFileIdsAsDictionary(self,inputFileName,columnId):
		"""
		getFileIdsAsDictionary

		This function reads a given input file and looks at the 
		column id and builds a dictionary of id values.

		"""
		print("   getFileIdsAsDictionary["+inputFileName+"]")
	
		#input file reader
		infile = open(inputFileName, "r")
		read = csv.reader(infile)
		next(read) #skip headers

		idDictionary={}

		#For each row
		for row in read:
			idValue = row[columnId]		
			idDictionary[idValue]=None
	
		infile.close()

		return idDictionary
	#/getFileIdsAsDictionary



	"""
	" segmentFile
	"""
	def segmentFile(self,inputFileName,outputFileName,columnId,idDictionaryFilter):
		"""
		segmentFile

		This function reads an input file and looks at the column id.
		If the column id is contained in the given idDictionaryFilter	
		then the row is output to a new file. if not, the row is dropped.

		"""

		print("    segmentFile["+inputFileName+"]->["+outputFileName+"]")

		#input file reader
		infile = open(inputFileName, "r")
		read = csv.reader(infile)
		headers = next(read) # store headers

		#output file writer
		outfile = open(outputFileName, "w")
		write = csv.writer(outfile)
		write.writerow(headers) # write headers

		#For each row
		for row in read:
			idValue = row[columnId]		
			if idValue in idDictionaryFilter:
				write.writerow(row)
	#/segmentFile
		

	"""
	" chunkFile
	"""
	def chunkFile(self,inputFileName,columnId,numChunks):
		"""
		chunkFile

		This function reads an input file and chunks it
		into separate files based on the numChunksVariable and 
		the number of rows in the input file.

		# Algorithm

		This algorithm iterates over each line in the input file
		and outputs the line to a new output file based on the
		- number of chunked output files specified
		- while preserving a complete set of ids is not split across multiple files.


		The algorithm works by first calculating the number of rows in the input file.
		This is done by iterating over all lines in the file. 
		This is scalable to large files as the file data is not held in memory.
		Using this number, and the given numChunks variable, the number of items 
		per chunk can be calculated.
		numRowsPerChunk = totalNumRows / numChunks

		The input file is then iterated line by line outputting the rows to a new file.	
		The remained of a division calculation is used to determine when a new segment
		should be created, and thus if a new file to output to should be created or not.

		rowCounter % numRowsPerChunk

		The remained of the calculation is 0 if the rowCounter is divisible 
		exactly by the numRowsPerChunk. Which indicates that the rowCounter
		is at the end of the number of rows that should be added to a file.
		

		## Example of reminder calculation

		A simple example of the remainder calculation is show below.
		The file has 6 rows, the number of chunks specified is 3. 
		Therefore we should chunk the input file into 3 files, each containing
		2 rows. Not considering headers.

		 numChunks       = 3
		 totalNumRows    = 6
		 numRowsPerChunk = 2

		0
		rowCounter(0) % numRowsPerChunk(2) = 0
		NewFile 1
		Write row to file num 1


		1
		rowCounter(1) % numRowsPerChunk(2) = 1
		chunkFileCounter = 1
		Write row to file num 1


		2
		rowCounter(2) % numRowsPerChunk(2) = 0
		NewFile 2
		Write row to file num 2


		3
		rowCounter(3) % numRowsPerChunk(2) = 1
		Write row to file num 2


		4
		rowCounter(4) % numRowsPerChunk(2) = 0
		 NewFile 3
		Write row to file num 3


		5
		rowCounter(5) % numRowsPerChunk(2) = 1
		Write row to file num 3.csv


		## Uneven file distribution

		The algorithm has a logical check to make sure that an uneven distribution
		of rows are still divided into the specified number of files given
		The if statement ensures that any extra rows are simply added to the last
		file. This statement only created a new file while the number of files
		do not exceed the number of chunks.

		if not chunkFileCounter>numChunks


		## Preserve ids in single files

		The algorithm also have a logical check to make sure that a set of ids is not
		split across multiple files.
		This part of the algorithm helps preserve that a set of ids for a single
		patient is kept to one file, and is not partially split amongst different files.

		This works by checking the value of the id column of the previous row.
		if the same and current row, then dont split to a new file.
		if the current row value is different, then it is fine to split into different files.
		
		This feature is auto enabled for all file chunking using this methods as it is an 
		important requirement for this application.


		## Pseudocode	

		The algorithm Pseudocode code which combines all of the concepts 
		explained above in to the algorithm.


		For each row in input

			currentIdValue=row[columnId]			

			if rowCounter % numRowsPerChunk == 0:				
				chunkFileCounter=chunkFileCounter+1

				if not chunkFileCounter>numChunks:
					createNewFileFlag=True
	
			if createNewFileFlag:

				# if current and previous id not equal, then is a new person.
				#    and we not exceeding the max number of file chunks
				#    then create a new file
				if (not previousIdValue == currentIdValue) and (not chunkFileCounter>numChunks):
	
					currentFileName = update file name using chunkFileCounter
					currentFileWriter = getNewFileWriter(currentFileName)
					createNewFileFlag=False

			currentFileWriter.writerow(row) 		
			rowCounter=rowCounter+1 
			previousIdValue=currentIdValue
		Endfor	



		# Function output 

		This function returns list of chunkedFileNames as the output.
		This is then used for further segmentation processes in the 
		main method.



		"""
		print(" chunkFile["+inputFileName+"]")

		FILE_SEGMENT_SUFFIX="seg"
		chunkedFileNames=[]

		#input file reader
		infile = open(inputFileName, "r")
		read = csv.reader(infile)
		next(read) # skip header
		
		totalNumRows = sum(1 for row in read) 
		numRowsPerChunk = totalNumRows / numChunks
		
		print("  numChunks       = "+str(numChunks))
		print("  totalNumRows    = "+str(totalNumRows))

		extras=""
		if not totalNumRows % numChunks ==0: extras=" ~ totalNumRows not exactly divisable by numChunks. remainders added to last file."
		print("  numRowsPerChunk = "+str(numRowsPerChunk)+extras)


		# Algorithm setup 

		# reset reading csv file
		infile.seek(0)
		headers = next(read) # store headers
		
		rowCounter=0
		chunkFileCounter=0

		currentFileName=""
		currentFileWriter=None

		currentIdValue=None
		previousIdValue=None

		createNewFileFlag=False


		# Init

		"""
		# create new file
		currentFileName=inputFileName.replace('.csv','.'+FILE_SEGMENT_SUFFIX+str(1)+'.csv')
		chunkedFileNames.append(currentFileName)

		currentFileWriter = self.getNewFileWriter(currentFileName)
		currentFileWriter.writerow(headers) # write headers
		"""

		#For each row
		for row in read:
			currentIdValue=row[columnId]			
			if rowCounter%numRowsPerChunk==0:				
				chunkFileCounter=chunkFileCounter+1

				if not chunkFileCounter>numChunks:
					createNewFileFlag=True
	
			if createNewFileFlag:

				# if current and previous id not equal, then is a new person.
				#    and we not exceeding the max number of file chunks
				#    then create a new file
				if (not previousIdValue == currentIdValue) and (not chunkFileCounter>numChunks):
	
					# create new file
					currentFileName=inputFileName.replace('.csv','.'+FILE_SEGMENT_SUFFIX+str(chunkFileCounter)+'.csv')
					chunkedFileNames.append(currentFileName)

					currentFileWriter = self.getNewFileWriter(currentFileName)
					currentFileWriter.writerow(headers) # write headers

					createNewFileFlag=False


			# write row
			currentFileWriter.writerow(row)

			# increment rowCounter
			rowCounter=rowCounter+1

			# id value update
			previousIdValue=currentIdValue

		infile.close()
		return chunkedFileNames
	#/chunkFile




	"""
	" getNewFileWriter
	"""
	def getNewFileWriter(self,outputFileName):		
		outfile = open(outputFileName, "w")
		write = csv.writer(outfile)		
		return write
	#/getNewFileWriter



"""
" Runs this class
"
"""
if __name__ == '__main__':
	print("Starting")	
	print
	start_time = time.time()
	
	parser = ConfigParser.SafeConfigParser()
	parser.read(sys.argv[1])


	#mainFileName
	mainFileName = parser.get('mainFile', 'mainFileName')
	mainFileColumnId = int(parser.get('mainFile', 'mainFileColumnId'))
	
	mainFileNumChunks=False
	if parser.has_option('mainFile', 'numChunks'):
		mainFileNumChunks = int(parser.get('mainFile', 'numChunks'))
	
	#supportingFileNames
	supportingFileNames = parser.get('supportingFiles', 'supportingFileNames').split('\n')

	supportingFileColumnIds = parser.get('supportingFiles', 'supportingFileColumnIds').split('\n')
	supportingFileColumnIds = map(int, supportingFileColumnIds) # cast list of strings to int


	print("Params: Main")
	print(" mainFileName           = "+mainFileName)
	print(" mainFileColumnId       = "+str(mainFileColumnId))

	if mainFileNumChunks:
		print(" mainFileNumChunks      = "+str(mainFileNumChunks))
	else:
		print(" mainFileNumChunks      = No file chunking specified")

	print
	print("Params: Supporting Files")		
	for i in range(0,len(supportingFileNames)):
		print(" "+str(i)+": supportFileName     = "+supportingFileNames[i] )
		print("    supportFileColumnId = "+str(supportingFileColumnIds[i]) )
	print


	fsa = FileSegmentationApp()	
	fsa.main(mainFileName,mainFileColumnId,mainFileNumChunks,supportingFileNames,supportingFileColumnIds)

	print("--- %s seconds ---" % (time.time() - start_time))
	print("done")

