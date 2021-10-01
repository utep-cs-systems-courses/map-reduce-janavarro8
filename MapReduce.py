"""
Javier Navarro
Map Reduce Assignment
"""

import pymp
import time

def main():
    fileNames = ["shakespeare1.txt", "shakespeare2.txt", "shakespeare3.txt", "shakespeare4.txt",
                "shakespeare5.txt", "shakespeare6.txt", "shakespeare7.txt", "shakespeare8.txt"]
    wordList = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet",
                "you", "my", "blood", "poison", "macbeth", "king", "heart", "honest"]

    start = time.monotonic()

    # open all the files in 'filenames'
    openFiles = []
    for file in fileNames:
        openFiles.append(open(file, 'r'))

    fileStart = time.monotonic()
    fileList = fileIntoList(openFiles)
    fileEnd = time.monotonic()
    fileElapsed = fileEnd - fileStart

    countStart = time.monotonic()
    countWords = mapreduce(fileList, wordList)
    countEnd = time.monotonic()
    countElapsed = countEnd - countStart
    print(countWords)

    for file in openFiles:
        file.close()

    end = time.monotonic()
    elapsed = end - start
    print("\nTime taken to read from files: " + str(fileElapsed))
    print("Time taken to count words: " + str(countElapsed))
    print("Total program runtime: " + str(elapsed))

"""
Receives a list of files and a list of words.
Distributes the list of files to available processes
and counts the occurences in each file of each word
into a dictionary.
Returns the dictionary of word counts.
"""
def mapreduce(fileList, wordList):
    sharedDict = pymp.shared.dict()
    with pymp.Parallel() as p:
        myLock = p.lock
        # give each process a file
        for fileWords in p.iterate(fileList):
            # to compare words of file to wordList
            for word in fileWords:
                for listWord in wordList:
                    if listWord in word.lower():    # compare ignoring case
                        myLock.acquire()
                        if listWord not in sharedDict: # if not initialized
                            sharedDict[listWord] = 0
                        sharedDict[listWord] += 1
                        myLock.release()
    return sharedDict

"""
Receives a list of open files,
reads the files and puts the words into a list.
Returns a 2D list containing the words of each file.
"""
def fileIntoList(openFiles):
    fileList = []
    for file in openFiles:
        currentFile = []
        for line in file:
            for word in line.split(): # break down file to each word in file
                currentFile.append(word)
        fileList.append(currentFile) # append list of words to list of files
    return fileList

if __name__ == '__main__':
    main()
