import sys
import itertools
import math
from operator import add
from pyspark import SparkContext

def split_input_line(line):
	line = (line.strip() + ' ')
	docid, swords = line.split(': ')
	words = swords.split(' ')
	return [(docid, word) for word in words]

def docid_word_one(val):
	docid, word = val
	return (u'@'.join((docid, word)).encode('utf-8').strip(), 1)

def word_docid_count(val):
	docid_word, count = val
	docid, word = docid_word.split('@')
	return (word, '{0}@{1}'.format(docid, count))

def word_docid_count_docswithword(val):
	word = val[0]
	docid_count_pairs = []
	counter = 0
	for docid_count in val[1]:
		counter = counter + 1
		docid, count = docid_count.split('@')
		docid_count_pairs.append((docid, count))
	result = []
	for (docid, count) in docid_count_pairs:
		word_docid = '{0}@{1}'.format(word, docid)
		count_docswithword = '{0}@{1}'.format(count, counter)
		result.append((word_docid, count_docswithword))
	return result

def count_tf_idf(val):
	word_doc, count_docswithword = val
	word = word_doc.split('@')[0]
	fdt, dft = [int(x) for x in count_docswithword.split('@')]
	return (fdt * math.log(docs_count / dft), word)

sc = SparkContext(appName="tf-idf")

files = sc.wholeTextFiles(sys.argv[1])
need_doc_id = int(sys.argv[2])
docs_count = int(sys.argv[3])

doc_word_pairs = files.map(lambda x: x[-1]) \
                      .flatMap(split_input_line) \
                      .map(docid_word_one) \
                      .reduceByKey(add) \
                      .map(word_docid_count) \
                      .groupByKey() \
                      .flatMap(word_docid_count_docswithword) \
                      .filter(lambda x: x[0].endswith('@{0}'.format(need_doc_id))) \
                      .map(count_tf_idf) \
                      .sortByKey(False) \
                      .collect()


it = 0
for (doc, word) in doc_word_pairs:
	it = it + 1
	print("{0} {1}".format(doc, word))
	if it == 20:
		break
sc.stop()